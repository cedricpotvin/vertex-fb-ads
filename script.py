import urllib.request, urllib.parse
import json
import os
import time
import pygsheets

def handler(event, context):

    access_token = os.environ.get("access_token")
    spreadsheet_id = os.environ.get("spreadsheet_id")
    print("Starting")
    gc = pygsheets.authorize(service_account_env_var='GDRIVE_API_CREDENTIALS')
    print(gc)
    sh = gc.open_by_key(spreadsheet_id)
    print(sh.title)
    wk1 = sh.worksheet_by_title(os.environ.get("sheet_name"))
    print(wk1)
    output = []
    response = urllib.request.urlopen(
        'https://graph.facebook.com/v7.0/me?access_token=' + access_token + '&fields=adaccounts{ads{insights{ad_name},status,offline_conversion_data_sets{customconversions}}}')
    ads_accounts = json.load(response)

    for ad_account in ads_accounts['adaccounts']['data']:
        if "ads" in ad_account:
            for ad in ad_account["ads"]["data"]:
                if "insights" in ad and ad["status"] == 'ACTIVE':
                    url = 'https://graph.facebook.com/v7.0/' + ad[
                        "id"] + '/insights?fields=engagement_rate_ranking,quality_ranking,unique_clicks,campaign_name,adset_name,ad_name,reach,conversion_rate_ranking,account_name,cpc,unique_actions,action_values,spend,clicks&access_token=' + access_token + '&date_preset=last_7d'
                    error = False
                    first_request = True
                    while error or first_request:
                        try:
                            time.sleep(5)
                            response = urllib.request.urlopen(url)
                            first_request = False
                            error = False
                        except:
                            time.sleep(5)
                            error = True
                            print("ERROR")
                    response = json.load(response)
                    if response["data"]:
                        if "cpc" in response["data"][0]:
                            response["data"][0]["cpc"] = round(float(response["data"][0]["cpc"]), 2)
                        if "unique_actions" in response["data"][0]:
                            for action in response["data"][0]["unique_actions"]:
                                response["data"][0][action["action_type"]] = action["value"]
                        output.append(response["data"][0])

    row = 1
    for item in output:
        values = [
            item.get("account_name"),
            item.get("campaign_name"),
            item.get("adset_name"),
            item.get("ad_name"),
            item.get("reach"),
            item.get("clicks"),
            item.get("cpc"),
            item.get("quality_ranking"),
            item.get("engagement_rate_ranking"),
            item.get("conversion_rate_ranking"),
            item.get("unique_clicks"),
            item.get("spend"),
            item.get("offsite_conversion"),
            item.get("link_click"),
            item.get("landing_page_view"),
            item.get("post_reaction"),
            item.get("post_engagement"),
            item.get("page_engagement"),
            item.get("lead")
        ]
        print(values)

        k = wk1.insert_rows(row=row, values=values)
        print(k)

if __name__ == "__main__":
    handler({
        'record_id': 'testrecord123',
        'dry_run': True
    }, dict())