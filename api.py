import asyncio
import aiohttp

from typing import Literal

next_delay = 0


async def _parse_hudoc_response(response, results: Literal['top', 'all']):
    """
    Helper to uniformly parse HUDOC responses
    """
    if response.status != 200:
        print(response)
        return None

    response = await response.json()

    resultcount = response['resultcount']

    if resultcount == 0:
        return None

    if results == 'top':
        return response['results'][0]['columns']
    elif results == 'all':
        return [data['columns'] for data in response['results']]

    raise ValueError("Invalid parameters")


async def _query_hudoc(url: str, results: Literal['top', 'all']):
    connector = aiohttp.TCPConnector(limit_per_host=1)

    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url) as response:
            return await _parse_hudoc_response(response, results=results)


async def hudoc_judgment_metadata(by: Literal['ecli', 'appno'], case: str, delay: float = 0):
    """
    Fetch HUDOC metadata_annotations based on either 1) ECLI number or 2) application number.
    NB: only CASE metadata_annotations is returned. Decisions etc. are not returned.
    """
    global next_delay

    if delay:
        next_delay += delay
        await asyncio.sleep(next_delay)

    if by == 'ecli':
        ecli_encoded = case.replace(':', '%3A')
        url = f"https://hudoc.echr.coe.int/app/query/results?query=contentsitename%3AECHR%20AND%20(NOT%20(doctype%3DPR%20OR%20doctype%3DHFCOMOLD%20OR%20doctype%3DHECOMOLD))%20AND%20((languageisocode%3D%22ENG%22))%20AND%20((ecli%3A%22{ecli_encoded}%22))%20AND%20((documentcollectionid%3D%22JUDGMENTS%22))&select=sharepointid,Rank,ECHRRanking,languagenumber,itemid,docname,doctype,application,appno,conclusion,importance,originatingbody,typedescription,kpdate,kpdateAsText,documentcollectionid,documentcollectionid2,languageisocode,extractedappno,isplaceholder,doctypebranch,respondent,advopidentifier,advopstatus,ecli,appnoparts,sclappnos&sort=&start=0&length=20&rankingModelId=11111111-0000-0000-0000-000000000000"
    elif by == 'appno':
        appno_encoded = case.replace('/', '%2F')
        url = f"https://hudoc.echr.coe.int/app/query/results?query=contentsitename%3AECHR%20AND%20(NOT%20(doctype%3DPR%20OR%20doctype%3DHFCOMOLD%20OR%20doctype%3DHECOMOLD))%20AND%20((languageisocode%3D%22ENG%22))%20AND%20((appno%3A%22{appno_encoded}%22))%20AND%20((documentcollectionid%3D%22JUDGMENTS%22))&select=sharepointid,Rank,ECHRRanking,languagenumber,itemid,docname,doctype,application,appno,conclusion,importance,originatingbody,typedescription,kpdate,kpdateAsText,documentcollectionid,documentcollectionid2,languageisocode,extractedappno,isplaceholder,doctypebranch,respondent,advopidentifier,advopstatus,ecli,appnoparts,sclappnos&sort=&start=0&length=20&rankingModelId=22222222-eeee-0000-0000-000000000000"
    else:
        raise ValueError("Invalid parameters")

    return await _query_hudoc(url, results='top')


async def hudoc_judgments_metadata(
    by: Literal['ecli', 'appno'],
    cases: list[str],
    delay: float = 0.02,
    output: bool = True
):
    """
    Fetch HUDOC metadata_annotations for a list of cases.
    NB: there are a lot of requests, and some may fail - thus the recursive implementaion :-)
    """
    if not ((by == 'ecli') or (by == 'appno')):
        raise ValueError("Invalid parameters")

    failed_cnt = [0]
    citations_metadata = []

    async def _fetch(_cases):
        global next_delay

        if output:
            print(f">> Start iteration for {len(_cases)} {by.upper()}s.")
        next_delay = 0

        if not _cases:
            return

        # Construct HUDOC queries
        _tasks = [hudoc_judgment_metadata(by=by, case=_case, delay=delay) for _case in _cases]

        # Execute HUDOC queries
        responses = await asyncio.gather(*_tasks)

        for res in responses:
            citations_metadata.append(res)

        failed_appnos = [appno for appno, result in zip(cases, citations_metadata) if not result]

        if len(failed_appnos) != failed_cnt[0]:
            failed_cnt[0] = len(failed_appnos)
            await _fetch(failed_appnos)

    await _fetch(cases)

    if output:
        print(f">> Done, could not improve further. "
              f"Identified {len(cases) - failed_cnt[0]} judgments in {len(cases)} {by.upper()}s.")

    return [citation for citation in citations_metadata if citation]


async def hudoc_judgment_incoming_citations_metadata(by: Literal['appno'], case: str, delay: float = 0):
    """
    Fetch INCOMING citations of HUDOC case. The metadata of the citing cases are returned.
    """
    global next_delay

    if delay:
        next_delay += delay
        await asyncio.sleep(next_delay)

    if by == 'appno':
        appno_encoded = case.replace('/', '%2F')
        url = f"https://hudoc.echr.coe.int/app/query/results?query=contentsitename%3AECHR%20AND%20(NOT%20(doctype%3DPR%20OR%20doctype%3DHFCOMOLD%20OR%20doctype%3DHECOMOLD))%20AND%20((languageisocode%3D%22ENG%22))%20AND%20((documentcollectionid%3D%22JUDGMENTS%22))%20AND%20((scl%3A%22{appno_encoded}%22))&select=sharepointid,Rank,ECHRRanking,languagenumber,itemid,docname,doctype,application,appno,conclusion,importance,originatingbody,typedescription,kpdate,kpdateAsText,documentcollectionid,documentcollectionid2,languageisocode,extractedappno,isplaceholder,doctypebranch,respondent,advopidentifier,advopstatus,ecli,appnoparts,sclappnos&sort=&start=0&length=20&rankingModelId=11111111-0000-0000-0000-000000000000"
    else:
        raise ValueError("Invalid parameters")

    return await _query_hudoc(url, results='all')


async def hudoc_judgments_incoming_citations_metadata(
    by: Literal['appno'],
    cases: list[str],
    delay: float = 0.02
):
    """
    Fetch metadata of incoming citations to a list of application numbers
    NB: there are a lot of requests, and some may fail - thus the recursive implementaion :-)
    """
    failed_cnt = [0]
    incoming_citations = []

    async def _fetch(_cases):
        global next_delay

        print(f">> Start iteration for {len(_cases)} {by.upper()}s.")
        next_delay = 0

        if not _cases:
            return

        # Construct HUDOC queries
        _tasks = [hudoc_judgment_incoming_citations_metadata(by=by, case=_case, delay=delay) for _case in _cases]

        # Execute HUDOC queries
        responses = await asyncio.gather(*_tasks)

        for res in responses:
            incoming_citations.append(res)

        failed_appnos = [appno for appno, result in zip(cases, incoming_citations) if not result]

        if len(failed_appnos) != failed_cnt[0]:
            failed_cnt[0] = len(failed_appnos)
            await _fetch(failed_appnos)

    await _fetch(cases)

    print(f">> Done, could not improve further. "
          f"Found incoming citations for {len(cases) - failed_cnt[0]} judgments in {len(cases)} {by.upper()}s.")

    return [citation for citation in incoming_citations if citation]