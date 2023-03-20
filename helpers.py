import api
import pandas as pd

from typing import Literal


async def construct_outgoing_citations_dataframe(
    df_metadata,
    generations: list[int],
):
    citations_unfiltered = set()  # Union of all SCL and extracted appnos

    for col in ['sclappnos', 'extractedappno']:
        for appnos in df_metadata[col]:
            for appno in appnos:
                citations_unfiltered.add(appno)

    new_cases_metadata = await api.hudoc_judgments_metadata(
        by='appno', cases=list(citations_unfiltered), delay=0.02
    )

    # Map ECLI to metadata
    citations_metadata_ecli_map = dict()

    for d in new_cases_metadata:
        ecli = d['ecli']
        if ecli not in citations_metadata_ecli_map:
            citations_metadata_ecli_map[ecli] = d

    # Construct empty DataFrame
    df_citations = pd.DataFrame(index=df_metadata.index, columns=citations_metadata_ecli_map.keys())

    # Populate DataFrame with citations
    for ecli, data in df_metadata.iterrows():

        # Metadata for that specific ECLI number
        metadata_for_ecli = df_metadata.loc[ecli]
        own_appno = metadata_for_ecli['appno'].split(';')

        # Mark citations in DF
        for col in ['sclappnos', 'extractedappno']:
            for appno in data[col]:

                # A case cannot cite itself :-)
                if appno not in own_appno:

                    # If no metadata found for appno, that means the appno does not correspond to a JUDGMENT.
                    metadata_for_appno = list(
                        filter(lambda t: appno in t['appno'].split(';'), citations_metadata_ecli_map.values())
                    )
                    if not metadata_for_appno:
                        continue

                    if len(metadata_for_appno) != 1:
                        # raise Exception("Could not find metadata for APPNO.")
                        continue
                    metadata_for_appno = metadata_for_appno[0]

                    df_citations.at[ecli, metadata_for_appno['ecli']] = True

    df_citations = df_citations.dropna(axis=1, how='all')
    df_citations = df_citations.dropna(axis=0, how='all')

    print(f"\nGeneration(s) [{' & '.join([str(g) for g in generations])}] "
          f"cited {len(df_citations.columns)} unique cases.")

    return df_citations, citations_metadata_ecli_map


def convert_ecli_to_case_names(df_citations, citing_cases_metadata, citations_metadata_ecli_map):
    df_citations_named = df_citations.copy(deep=True)

    df_citations_named.index.name = 'case_name'

    # TODO: improve speed of bottom 2 for loops

    # Rename columns
    for col in df_citations_named.columns:
        if 'ECLI:CE:ECHR' not in col:
            continue
        df_citations_named.rename({
            col: citations_metadata_ecli_map[col]['docname']
        }, inplace=True, axis=1)

    # Rename rows
    for row in df_citations_named.index:
        if 'ECLI:CE:ECHR' not in row:
            continue
        df_citations_named.rename({
            row: list(filter(lambda x: row == x['ecli'], citing_cases_metadata))[0]['docname']
        }, inplace=True, axis=0)


    df_citations_named = df_citations_named.fillna(False)

    # Merge duplicate names (rows)
    df_citations_named = df_citations_named.groupby(level=0).sum()

    # merge duplicate names (columns)
    df_citations_named = df_citations_named.groupby(level=0, axis=1).sum()

    # NB: because of sum (i.e. above two assignments), the values have been converted to integers.
    df_citations_named = df_citations_named.astype(bool)

    # Make sure 'generation 0' and 'generation 1' (etc.) are the first columns of the DataFrame
    df_citations_named = df_citations_named[df_citations_named.columns.sort_values(ascending=False)]

    return df_citations_named


def nodes_edges(df_citations):
    """ Construct nodes and edges for a citations DataFrame """

    # Remove duplicates - these can occur if a case is both citing and cited
    node_list = list(set([col for col in df_citations.index.tolist() + df_citations.columns.tolist()
                     if not col.startswith('generation')]))

    ## NODES
    case_nodes = pd.DataFrame({
        'Label': node_list
    })

    case_nodes.index.name = 'Id'

    ## EDGES
    case_edges = pd.DataFrame(columns=['Source', 'Source Label', 'Target', 'Target Label', 'Type'])

    for case_name, cases in df_citations.iterrows():
        cited_cases = cases[cases == True].index.tolist()

        for cited_case_name in cited_cases:
            if cited_case_name.startswith('generation'):
                continue

            case_edges = pd.concat([case_edges, pd.DataFrame({
                'Source': case_nodes[case_nodes['Label'] == case_name].index[0],
                'Target': case_nodes[case_nodes['Label'] == cited_case_name].index[0],
                'Source Label': case_name,
                'Target Label': cited_case_name,
                'Type': 'Directed',
            }, index=[0])], ignore_index=True)

    ## GENERATIONS (TODO: speed up)
    generations = sorted([col for col in df_citations.columns.tolist() if col.startswith('generation')])

    for idx, case_name in enumerate(node_list):
        for generation in generations:
            try:
                case_nodes.at[idx, generation] = df_citations.loc[case_name][generation]
            except KeyError:
                # Only for next-generation cases that are not yet in df_citations's
                # rows, (and thus have no "existing" generation)
                next_generation = f"generation {int(generations[-1][-1]) + 1}"
                case_nodes.at[idx, next_generation] = True

    # Fill NaN's in generation columns with default (False)
    case_nodes.fillna(False, inplace=True)

    return case_nodes, case_edges