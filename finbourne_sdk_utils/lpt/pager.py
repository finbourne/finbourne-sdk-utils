import pandas as pd
import re
import urllib.parse

from finbourne_sdk_utils.lpt.either import Either

rexp = re.compile(r".*page=([^=']{10,}).*")


def page_all_results(fetch_page, page_handler):
    results = []

    def got_page(result):
        results.append(page_handler(result))

        links = [link for link in result.content.links if link.relation == "NextPage"]

        if len(links) > 0:
            match = rexp.match(links[0].href)
            if match:
                return urllib.parse.unquote(match.group(1))
        return None

    page = Either(None)
    while True:
        page = fetch_page(page.right).bind(got_page)
        if page.is_left():
            return page
        if page.right is None:
            break

    return pd.concat(results, ignore_index=True, sort=False)
