from PyPDF2 import PdfReader
import pandas as pd
import numpy as np
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaIoBaseUpload
from google.cloud import bigquery
from io import BytesIO
import gspread
import google.auth
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


def get_site_to_sites():
    creds, project_id = google.auth.default(
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    sheets_client = gspread.authorize(creds)
    site_to_sites = pd.DataFrame(
        sheets_client.open_by_key("1VNrT_Bf5JrZg_AksLvDkN7_Du7SBaWk6I-_SAH70KQQ")
        .worksheet("SiteToSites")
        .get_all_records()
    )
    return site_to_sites


def get_cmp_adunits_sheet():
    creds, project_id = google.auth.default(
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    sheets_client = gspread.authorize(creds)
    df = pd.DataFrame(
        sheets_client.open_by_key("10nMQHNg-kltIb-UuWujIFwOxdauWMkyIuh4utgkeXiI")
        .worksheet("sites")
        .get_all_records()
    )
    return df


def get_cmp_invoice_overall():
    creds, project_id = google.auth.default(
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    sheets_client = gspread.authorize(creds)
    df = pd.DataFrame(
        sheets_client.open_by_key("1w1mTEteqxwZw-B4RILuNincPuN8oSBv-qzdCTbni9Xk")
        .worksheet("CMP")
        .get_all_records()
    )
    return df


def get_bq_data(start_date=None, end_date=None):
    project_id = "dev-era-184513"
    client = bigquery.Client(project=project_id)
    if (start_date) and (end_date):
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        sql_query = f"""SELECT publisher_site, source_name, creative_size, creative_type, service_type, sum(paid_impressions) as imp, sum(raw_revenue) as raw_rev, sum(traded_revenue) as traded_rev, sum(adjusted_revenue) as adj_rev, sum(net_revenue) as net_rev  
                FROM `dev-era-184513.massarius_data.combined_net` 
                WHERE Date(date)>='{start_date}' and date<='{end_date}'
                GROUP BY publisher_site, source_name,  creative_size, creative_type, service_type
        """

    query_job = client.query(sql_query)
    df = query_job.result().to_dataframe()
    return df


def duplicates_removal(merged_df, duplicate_pub):
    df5 = merged_df[merged_df["url"] == duplicate_pub].copy()
    df5 = df5.drop_duplicates()
    merged_df = merged_df[merged_df["url"] != duplicate_pub]
    frames = [df5, merged_df]
    merged_df = pd.concat(frames)
    del df5
    return merged_df


def merge_data(df, site_to_sites):
    import pandas as pd

    df["publisher_site_lower"] = df["publisher_site"].str.lower()
    site_to_sites["url_lower"] = site_to_sites["url"].str.lower()
    merged_df = pd.merge(
        df,
        site_to_sites[["url_lower", "url", "billingcode"]],
        left_on="publisher_site_lower",
        right_on="url_lower",
        how="left",
    )
    list_of_duplicates = ["nederland.fm", "wikihealth.gr", "supporters.nl"]
    for i in range(0, len(list_of_duplicates)):
        merged_df = duplicates_removal(merged_df, list_of_duplicates[i])
    merged_df["aggregated_net_revenue"] = merged_df.groupby("billingcode")[
        "net_rev"
    ].transform("sum")
    merged_df = merged_df[["billingcode", "aggregated_net_revenue"]]
    merged_df = merged_df.drop_duplicates(subset=["billingcode"], keep="last")
    merged_df = merged_df.reset_index(drop=True)
    return merged_df


def get_pubs_shares():
    # Cloud Function
    creds, project_id = google.auth.default(
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    sheets_client = gspread.authorize(creds)
    subpub_shares = pd.DataFrame(
        sheets_client.open_by_key("1VNrT_Bf5JrZg_AksLvDkN7_Du7SBaWk6I-_SAH70KQQ")
        .worksheet("subpub_shares")
        .get_all_records()
    )
    return subpub_shares


def invoice_linebyline_extractor(
    google_drive_client, file_id, file_name
):  # filename=None, filepath=None

    file = get_from_google_drive(google_drive_client, file_id, file_name)

    reader = PdfReader(file)
    page = reader.pages[0]

    one_string = page.extract_text()
    one_string = one_string.replace("€", "")
    one_string = one_string.replace("⓿", " ⓿ ")
    one_string = one_string.replace("①", " ① ")
    one_string = one_string.replace("❶", " ❶ ")
    one_string = one_string.replace("❷", " ❷ ")
    one_string = one_string.replace("◊", " ◊ ")
    one_string = one_string.replace("⌂", " ⌂ ")
    one_string = one_string.replace("□", " □ ")
    split_str = one_string.split()
    rows = one_string.split("\n")
    return rows


def wacp_calculation(
    pub_shares_dict, traded_revenue, display, direct, video, inapp, richmedia
):
    # TODO: find a way to notify about the zeros
    # Make a Warning (by also adding the pub) -> for those for which there is revenue BUT there's no share recorded
    # Example -> pub_shares_dict={'InApp': 0.3, 'Direct': 0.35, 'Display': 0.3, 'Video': 0.35}
    # {'Direct': 0.25, 'Rich Media': 0.3, 'Video': 0.25, 'Display': 0.2, 'Inapp': 0.3}
    # inapp
    try:
        inapp_share = pub_shares_dict["InApp"]
    except:
        try:
            inapp_share = pub_shares_dict["Inapp"]
        except:
            try:
                inapp_share = pub_shares_dict["inApp"]
            except:
                inapp_share = 0
    try:
        direct_share = pub_shares_dict["Direct"]
    except:
        direct_share = 0
    try:
        display_share = pub_shares_dict["Display"]
    except:
        display_share = 0
    try:
        video_share = pub_shares_dict["Video"]
    except:
        video_share = 0
    try:
        richmedia_share = pub_shares_dict["Rich_Media"]
    except:
        try:
            richmedia_share = pub_shares_dict["Richmedia"]
        except:
            try:
                richmedia_share = pub_shares_dict["Rich Media"]
            except:
                richmedia_share = 0

    # TODO: The outcome of weighted_average_commission_percentage must be limited to 4 decimals
    weighted_average_commission_percentage = (
        (display_share * (display / traded_revenue))
        + (direct_share * (direct / traded_revenue))
        + (video_share * (video / traded_revenue))
        + (inapp_share * (inapp / traded_revenue))
        + (richmedia_share * (richmedia / traded_revenue))
    )
    return (
        weighted_average_commission_percentage,
        inapp_share,
        direct_share,
        display_share,
        video_share,
        richmedia_share,
    )


def getAllFileNames(BFfolder="", version="", subfolder=""):
    import os

    """This function returns a list of all file named in the given directory."""
    if subfolder == "":
        # C:\Users\GeorgeBaltzopoulosMa\Dropbox (Massarius)\MS Office - Company Info\pdfCheck
        #        path = os.path.join("C:",os.sep,"Users","NathalieHuizelingMas","Dropbox (Massarius)","MS Office NL - Billingfile", BFfolder, "verzonden", version)
        # TODO: Adjust V
        #        path = os.path.join("C:",os.sep,"Users","GeorgeBaltzopoulosMa","Dropbox (Massarius)","MS Office - Company Info","pdfCheck", BFfolder, "Verzonden", version)
        # Test on WSL on Windows so useless for Nathalie
        path = os.path.join(
            os.sep, "home", "georbalt", "code", "cloud", BFfolder, "verzonden", version
        )

    else:
        #        path = os.path.join("C:",os.sep,"Users","NathalieHuizelingMas","Dropbox (Massarius)","MS Office NL - Billingfile", BFfolder, "verzonden", version, subfolder)
        #        path = os.path.join("C:",os.sep,"Users","GeorgeBaltzopoulosMa","Dropbox (Massarius)","MS Office - Company Info","pdfCheck", BFfolder, "Verzonden", subfolder)
        path = os.path.join(
            os.sep,
            "home",
            "georbalt",
            "code",
            "cloud",
            BFfolder,
            "verzonden",
            version,
            subfolder,
        )

    print(os.listdir(path))

    paths = []
    for k in range(0, len(os.listdir(path))):
        paths.append(path)

    return os.listdir(path), paths  # list-of-Strings, all file names.


def get_referenced_dates(invoice_date="2023 02 February"):
    from datetime import datetime, timedelta

    year, month_num, month_name = invoice_date.split()
    month_num = datetime.strptime(month_name, "%B").month
    start_date = datetime(int(year), month_num, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date = next_month - timedelta(days=next_month.day)
    return start_date.date(), end_date.date()


def subpub_shares_calculation(subpub_shares, end_date, specific_invoice_name):
    subpub_subset = subpub_shares[subpub_shares["billingcode"] == specific_invoice_name]
    # TODO: Ensure that the following condition is met
    # TODO: subpub_subset['enddate'] may be 01/05/2023 while the end_date may be 30/04/2023 !!! Be careful, adjust it
    subpub_subset_sorted = subpub_subset[
        (subpub_subset["enddate"] == "") | (subpub_subset["enddate"] >= end_date)
    ]  #
    pub_shares_dict = dict(
        zip(subpub_subset_sorted["share_type"], subpub_subset_sorted["percentage"])
    )
    # print(pub_shares_dict)
    return pub_shares_dict


def invoice_type(i):
    if i.startswith("Inv_Ins_"):
        x = "Invoice Instructions"
    elif i.startswith("WYSIWYG_"):
        x = "WYSIWYG"
    elif i.startswith("nin"):
        x = "nin"
    else:
        x = "regular"

    return x


def convert_BFfolder_to_date(reference_date):
    from datetime import datetime

    try:
        # Parse the input string to extract the year and month
        date_obj = datetime.strptime(reference_date, "%Y %d %B")

        # Set the day to 1 to get the first day of the month
        date_obj = date_obj.replace(day=1)

        # Format the date as 'dd/mm/yyyy'
        formatted_date = date_obj.strftime("%d/%m/%Y")

        return formatted_date
    except ValueError:
        # Handle invalid input gracefully
        return "Invalid input format. Please provide a string in the format 'YYYY DD Month'."


def cpm_check(
    df_cmp_adunits,
    df_cmp_invoice_overall,
    reference_date,
    site_to_sites,
    specific_invoice_name,
    cmp_license,
):
    # before you check
    reference_date = convert_BFfolder_to_date(reference_date)

    # check 1
    # should it have cmp license ?
    list_check_1 = site_to_sites[site_to_sites["billingcode"] == specific_invoice_name][
        "url"
    ].to_list()
    try:
        # check_1=np.any(df_cmp_adunits[df_cmp_adunits['url'].isin(list_check_1)]['CMP'].values == 'GoogleFunding')
        check_for_all_combinations = df_cmp_adunits[
            df_cmp_adunits["url"].isin(list_check_1)
        ]["CMP"].values
        values_to_check = ["GoogleFunding", "ConsentManagerNet", "InMobi", "QuantCast"]
        check_1 = np.any(np.isin(check_for_all_combinations, values_to_check))

    except Exception as e:
        check_1 = e
    # check 2
    # does the invoice contain cmp license
    # cpm_amount=df_cmp_invoice_overall[(df_cmp_invoice_overall['billingcode']==specific_invoice_name)& (df_cmp_invoice_overall['first_of_month']==reference_date)]['amount'].values[0]
    cpm_amount = df_cmp_invoice_overall[
        (df_cmp_invoice_overall["billingcode"] == specific_invoice_name)
        & (df_cmp_invoice_overall["first_of_month"] == reference_date)
    ]["amount"].values.sum()
    try:
        if cmp_license == cpm_amount:
            check_2 = True
        else:
            check_2 = False
    except Exception as e:
        check_2 = e
    # check 3
    # Is it correct?

    # check 4

    return check_1, check_2, cpm_amount, cmp_license


def evelyns_support_calculation(
    weighted_average_commission_percentage,
    net_rev,
    compensation,
    other_projects,
    extra_rev,
    discrepancies,
    corrections,
    cos,
    ad_sense_mcm,
    massarius_commision,
    net_total_from_invoice,
):
    # TODO: This 0.25 should be replaced by the sheet -> pub share

    try:
        weighted_comission = weighted_average_commission_percentage * (
            -abs(extra_rev) + abs(discrepancies) + abs(corrections) + abs(cos)
        )  # the % should be taken by the lookups sheet->pubrevshare or something like this
    except:
        print("A")
    try:
        evelynscalculation_local = (
            net_rev
            + abs(extra_rev)
            - abs(discrepancies)
            - abs(ad_sense_mcm)
            - abs(corrections)
            - abs(cos)
            + weighted_comission
        )
    except:
        print("b")
    try:
        evelynsCalculation_local = (
            evelynscalculation_local + abs(compensation) + abs(other_projects)
        )  # -abs(massarius_commision)
    except:
        print("c")
    try:
        difference = evelynsCalculation_local - abs(net_total_from_invoice)
    except:
        print("d")
    return evelynsCalculation_local, difference, weighted_average_commission_percentage


def value_extractor(
    df_cmp_adunits,
    df_cmp_invoice_overall,
    reference_date,
    site_to_sites,
    subpub_shares,
    rows,
    specific_invoice_name,
    df_merged,
    file_name,
    filepath,
    version=None,
):
    dyo_list = [
        "belgie",
        "jaspersmedia",
        "1908",
        "aof",
        "radioportalinternational",
        "reachmo",
        "MarinusMedia",
        "philsumbler",
        "gomotion",
        
    ]  #'regio15',,'1908'
    tria_list = [
        "belgiefm",
        "biernet",
        "1908.nl",
        "1908.nl",
        "radioportal",
        "reachmore",
        "marinus media",
        "jackarmy",
        "azalerts",
    ]  #'regio15','1908.nl'
    # Step 1 net revenue extraction
    # 20240221_Massarius_NL_RadioportalInternational.pdf radioportalinternational
    print("invoice name before", specific_invoice_name)
    try:
        net_rev = df_merged[
            df_merged["billingcode"] == specific_invoice_name
        ].aggregated_net_revenue.values[0]
    except Exception as e:
        print(f"Specific Invoice Name {specific_invoice_name} - Mismatch in -->{e}")
        if specific_invoice_name in dyo_list:
            net_rev = df_merged[
                df_merged["billingcode"]
                == tria_list[dyo_list.index(specific_invoice_name)]
            ].aggregated_net_revenue.values[0]
            # 1908 fix
            specific_invoice_name = tria_list[dyo_list.index(specific_invoice_name)]

    list_english_terms = [
        "Niet-geregistreerde / extra omzet",
        "SSP Verschillen",
        "Omzet AdSense MCM Directe Uitbetaling aan Publisher",
        "Correctie",
        "Verkoopkosten",
        "Commissie Massarius",
    ]

    list_dutch_terms = [
        "Unrecorded / Additional Revenues",
        "SSP Discrepancies",
        "Revenue AdSense MCM Direct Payout to Publisher",
        "Correction",
        "Cost of Sales",
        "Commission Massarius",
    ]
    # Date Start
    for i in range(0, len(rows)):
        if ("Begin" in rows[i]) | ("Start" in rows[i]):
            start_date = rows[i].split()[-1]
            break
    # Date End
    for i in range(0, len(rows)):
        if ("Eind " in rows[i]) | ("End" in rows[i]):
            end_date = rows[i].split()[-1]
            break
    # print('date_string',end_date)
    # "2023-03-01" and date<"2023-04-01"
    date_string = start_date
    day, month, year = date_string.split("/")
    start_date = f"{year}-{month}-{day}"

    date_string = end_date
    day, month, year = date_string.split("/")
    end_date = f"{year}-{month}-{day}"
    # Step 2 extra rev = extra sale
    for i in range(0, len(rows)):
        if (list_english_terms[0] in rows[i]) | (list_dutch_terms[0] in rows[i]):
            text_term = rows[i].split()[-1]
            if text_term == "-":
                extra_rev = 0
            else:
                extra_rev = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # extra_rev=float(rows[i].split()[-1].replace(',',''))
            break
    # Step 3 discrepancies (source:grey section)
    for i in range(0, len(rows)):
        if (list_english_terms[1] in rows[i]) | (list_dutch_terms[1] in rows[i]):
            text_term = rows[i].split()[-1]
            if text_term == "-":
                discrepancies = 0
            else:
                discrepancies = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # discrepancies=float(rows[i].split()[-1].replace(',',''))
            break
    # Step 4 adsense MCM
    for i in range(0, len(rows)):
        # TODO: be careful it gives back a minus
        if (list_english_terms[2] in rows[i]) | (list_dutch_terms[2] in rows[i]):
            text_term = rows[i].split()[-1]
            if text_term == "-":
                ad_sense_mcm = 0
            else:
                ad_sense_mcm = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # ad_sense_mcm=float(rows[i].split()[-1].replace(',',''))
            break
    # print('step 4 done')
    # Step 5 Corrections / Correctie
    for i in range(0, len(rows)):
        if (list_english_terms[3] in rows[i]) | (list_dutch_terms[3] in rows[i]):
            # TODO: be careful it gives back a minus
            text_term = rows[i].split()[-1]
            if text_term == "-":
                corrections = 0
            else:
                corrections = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # corrections=float(rows[i].split()[-1].replace(',',''))
            break
    # Step 6 CoS (source:invoice)
    for i in range(0, len(rows)):
        if (list_english_terms[4] in rows[i]) | (list_dutch_terms[4] in rows[i]):
            # TODO: be careful it gives back a minus
            text_term = rows[i].split()[-1]
            if text_term == "-":
                cos = 0
            else:
                cos = float(rows[i].split()[-1].replace(".", "").replace(",", "."))
                # Regular Format Below
                # cos=float(rows[i].split()[-1].replace(',',''))
            break
    # print('step 6 done')
    # Step 7 commission
    for i in range(0, len(rows)):
        if (list_english_terms[5] in rows[i]) | (list_dutch_terms[5] in rows[i]):
            text_term = rows[i].split()[-1]
            if text_term == "-":
                massarius_commision = 0
            else:
                massarius_commision = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # massarius_commision=float(rows[i].split()[-1].replace(',',''))
            break
    # print('step 7 done')
    # Identify other projects
    # default value
    other_projects = 0
    for i in range(0, len(rows)):
        # TODO: maybe
        # ('Overige projecten en/of kosten' in rows[i]) |
        if ("Overige projecten" in rows[i]) | ("Other Projects" in rows[i]):
            text_term = rows[i].split()[-1]
            if text_term == "-":
                other_projects = 0
            else:
                other_projects = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # other_projects=float(rows[i].split()[-1].replace(',',''))
            break
    # print('step overige projects done')
    # Identify Net Total
    for i in range(0, len(rows)):
        if ("Totaal Netto" in rows[i]) | ("Net Total" in rows[i]):
            net_total_from_invoice = rows[i].split()[-1]
            if net_total_from_invoice == "-":
                net_total_from_invoice = 0
            else:
                net_total_from_invoice = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # net_total_from_invoice=float(rows[i].split()[-1].replace(',',''))
            break
    # print('step totaal netto done')
    # print(net_rev,other_projects,extra_rev,discrepancies,corrections,cos,ad_sense_mcm,massarius_commision)
    # Traded Revenue
    traded_revenue = 0
    for i in range(0, len(rows)):
        # TODO: maybe
        # ('Overige projecten en/of kosten' in rows[i]) |
        if "Traded revenue" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_traded_revenue = split_string.index("revenue") + 2
            string_traded_revenue = split_string[index_traded_revenue]
            if string_traded_revenue == "-":
                traded_revenue = 0
            else:
                traded_revenue = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # traded_revenue=float(rows[i].split()[-1].replace(',',''))
            break
    for i in range(0, len(rows)):
        if "Traded Revenue" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_traded_revenue = split_string.index("Revenue") + 2
            string_traded_revenue = split_string[index_traded_revenue]
            if string_traded_revenue == "-":
                traded_revenue = 0
            else:
                traded_revenue = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # traded_revenue=float(rows[i].split()[-1].replace(',',''))
            break

    # Video
    video = 0
    for i in range(0, len(rows)):
        if "Video" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_video = split_string.index("Video") + 2
            string_video = split_string[index_video]
            try:
                video = float(string_video.replace(".", "").replace(",", "."))
                # Regular Format Below
                # video=float(string_video.replace(',', ''))
            except:
                video = 0
            break

    # Direct
    direct = 0
    for i in range(0, len(rows)):
        if "Direct" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_direct = split_string.index("Direct") + 2
            string_direct = split_string[index_direct]
            try:
                direct = float(string_direct.replace(".", "").replace(",", "."))
                # Regular Format Below
                # direct=float(string_direct.replace(',', ''))
            except:
                direct = 0
            break
    # Display
    display = 0
    for i in range(0, len(rows)):
        if "Display" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_display = split_string.index("Display") + 2
            string_display = split_string[index_display]
            try:
                display = float(string_display.replace(".", "").replace(",", "."))
                # Regular Format Below
                # display=float(string_display.replace(',', ''))
            except:
                display = 0
            break

    # # In App
    inapp = 0
    for i in range(0, len(rows)):
        if "InApp" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_inapp = split_string.index("InApp") + 2
            string_inapp = split_string[index_inapp]
            try:
                inapp = float(string_inapp.replace(".", "").replace(",", "."))
                # Regular Format Below
                # inapp=float(string_inapp.replace(',', ''))
            except:
                inapp = 0
            break

    # # Rich Media
    richmedia = 0
    for i in range(0, len(rows)):
        if "Rich Media" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_richmedia = split_string.index("Media") + 2
            string_richmedia = split_string[index_richmedia]
            try:
                richmedia = float(string_richmedia.replace(".", "").replace(",", "."))
                # Regular Format Below
                # richmedia=float(string_richmedia.replace(',', ''))
            except:
                richmedia = 0
            break
            # if text_term=='-':
            #     massarius_commision=0
            # else:
            #     massarius_commision=float(rows[i].split()[-1].replace('.','').replace(',','.'))
            # break
    Adserver = 0
    for i in range(0, len(rows)):
        if "Adserver" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_Adserver = split_string.index("Adserver") + 2
            string_Adserver = split_string[index_Adserver]
            try:
                Adserver = float(string_Adserver.replace(".", "").replace(",", "."))
                # Regular Format Below
                # Adserver=float(string_Adserver.replace(',', ''))
            except:
                Adserver = 0
    cos = cos + Adserver

    juli_rev = 0
    for i in range(0, len(rows)):
        if "Juli 2023" in rows[i]:
            text_term1 = rows[i]
            split_string = text_term1.split()
            index_juli_rev = split_string.index("Juli 2023") + 2
            string_juli_rev = split_string[index_juli_rev]
            try:
                juli_rev = float(string_juli_rev.replace(".", "").replace(",", "."))
                # Regular Format Below
                # juli_rev=float(string_juli_rev.replace(',', ''))
            except:
                juli_rev = 0
    other_projects = other_projects + juli_rev

    addition = video + direct + display + inapp + richmedia
    # if addition-traded_revenue<0.02 or addition-traded_revenue:
    # flag if 100 > 100.01 (case: +0.01) or 100 < 99.99 (case: -0.01)

    # flag if -100 > -99.99 (case: +0.01) or -100 < -100.01 (case: -0.01)

    # Identify if there're any discrepancies
    if (addition > traded_revenue + 0.01) or (addition < traded_revenue - 0.01):
        flag = "Red"
    else:
        flag = "Blue"

    # Step Compensation
    # sfera (mibebeyyo)
    compensation = 0
    for i in range(0, len(rows)):
        # TODO: maybe
        # ('Overige projecten en/of kosten' in rows[i]) |
        if ("Compensation" in rows[i]) | ("Compensatie" in rows[i]):

            text_term = rows[i].split()[-1]
            if text_term == "-":
                compensation = 0
            else:
                compensation = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # compensation=float(rows[i].split()[-1].replace(',',''))
            break
    print(compensation)

    # CMP Licence

    cmp_license = 0
    for i in range(0, len(rows)):
        # TODO: maybe
        # ('Overige projecten en/of kosten' in rows[i]) |
        if ("CMP Licentie(s)" in rows[i]) | ("CMP Licence(s)" in rows[i]):
            text_term = rows[i].split()[-1]
            if text_term == "-":
                cmp_license = 0
            else:
                cmp_license = float(
                    rows[i].split()[-1].replace(".", "").replace(",", ".")
                )
                # Regular Format Below
                # cmp_license=float(rows[i].split()[-1].replace(',',''))#.replace('.','')
            break
    # specific_invoice_name
    check_1, check_2, cpm_amount, cmp_license = cpm_check(
        df_cmp_adunits,
        df_cmp_invoice_overall,
        reference_date,
        site_to_sites,
        specific_invoice_name,
        cmp_license,
    )

    # print('weighted_average_commission_percentage,net_rev,other_projects,extra_rev,discrepancies,corrections,cos,ad_sense_mcm,massarius_commision,net_total_from_invoice')
    # print(weighted_average_commission_percentage,net_rev,other_projects,extra_rev,discrepancies,corrections,cos,ad_sense_mcm,massarius_commision,net_total_from_invoice)
    try:
        pub_shares_dict = subpub_shares_calculation(
            subpub_shares, end_date, specific_invoice_name
        )
    except Exception as e:
        print(subpub_shares)
        print(end_date)
        print(specific_invoice_name)
    (
        weighted_average_commission_percentage,
        inapp_share,
        direct_share,
        display_share,
        video_share,
        richmedia_share,
    ) = wacp_calculation(
        pub_shares_dict, traded_revenue, display, direct, video, inapp, richmedia
    )
    # print('weighted_average_commission_percentage',weighted_average_commission_percentage)
    evelynsCalculation_local, difference, weighted_average_commission_percentage = (
        evelyns_support_calculation(
            weighted_average_commission_percentage,
            net_rev,
            compensation,
            other_projects,
            extra_rev,
            discrepancies,
            corrections,
            cos,
            ad_sense_mcm,
            massarius_commision,
            net_total_from_invoice,
        )
    )

    df_temp = pd.DataFrame(
        {
            "filename": [file_name],
            "filepath": [filepath],
            "version": [version],
            "billingcode": [specific_invoice_name],
            "start_date_invoice": [start_date],
            "end_date_invoice": [end_date],
            "traded_revenue": [traded_revenue],
            "revshare_display": [display_share],
            "display": [display],
            "revshare_direct": [direct_share],
            "direct": [direct],
            "revshare_video": [video_share],
            "video": [video],
            "revshare_inapp": [inapp_share],
            "inapp": [inapp],
            "revshare_richmedia": [richmedia_share],
            "richmedia": [richmedia],
            "net_revenue": [net_rev],
            "compensation": [compensation],
            "other_projects": [other_projects],
            "extra_revenue": [extra_rev],
            "discrepancies": [discrepancies],
            "corrections": [corrections],
            "Cost_Of_Sales": [cos],
            "ad_sense_mcm": [ad_sense_mcm],
            "massarius_commision": [massarius_commision],
            "evelyns_calculation": [evelynsCalculation_local],
            "net_total": [net_total_from_invoice],
            "difference_evelyn_-_net_total": [difference],
            "weighted_average_commission_percentage": [
                weighted_average_commission_percentage
            ],
            "should_have_cmp_license": check_1,
            "it_is_shown_corrently_on_the_invoice": check_2,
            "cpm_amount": cpm_amount,
            "cpm_license": [cmp_license],
        }
    )
    return df_temp


def google_client():
    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    # Use google.auth.default to get application default credentials (ADC)
    creds, project_id = google.auth.default(scopes=SCOPES)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    client = build("drive", "v3", credentials=creds)

    return client


def find_in_google_drive(client, BFfolder, version):

    # 2. Find the requested folder
    # main_folder_id
    drive_id = "0ANsdsK4IZ2XMUk9PVA"
    query = "mimeType='application/vnd.google-apps.folder' and trashed = false"
    query = f"'{drive_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = (
        client.files()
        .list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    items = results.get("files", [])
    for item in items:
        if item["name"] == BFfolder:
            main_folder_id = item["id"]
    # 3 find verzonden folder
    query = f"'{main_folder_id}' in parents and trashed = false"
    results = (
        client.files()
        .list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    items = results.get("files", [])
    for item in items:
        if item["name"].lower() == "verzonden":
            verzonden_folder_id = item["id"]
    # 4. find version folder
    query = f"'{verzonden_folder_id}' in parents and trashed = false"
    results = (
        client.files()
        .list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    items = results.get("files", [])
    for item in items:
        # TODO: adjust the version
        if item["name"].lower() == version:
            version_folder_id = item["id"]

    # 5.Find the rest of subfolders
    query = f"'{version_folder_id}' in parents and trashed = false"
    results = (
        client.files()
        .list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    items = results.get("files", [])
    subsubfolders_ids = []
    for item in items:
        # TODO: adjust the version
        if item["name"].lower() in ["speld", "nin"]:
            subsubfolders_ids.append(item["id"])
    # 6 join all subfolderids
    subsubfolders_ids.append(version_folder_id)
    # 7 all pdf naming conventions in one list
    all_results = []
    for folder_id in subsubfolders_ids:
        query = f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
        results = (
            client.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name, mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        results.get("files", [])
        for result in results["files"]:
            all_results.append(result)
    return all_results, version_folder_id


def get_from_google_drive(client, file_id, file_name):
    downloaded_file = download_file(client, file_id, file_name)
    return downloaded_file


def download_file(client, file_id, file_name):
    request = client.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh


def upload_file(client, folder_id, df, file_name):
    file_stream = BytesIO()
    df.to_excel(file_stream, index=False, engine="openpyxl")
    file_stream.seek(0)
    file_metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaIoBaseUpload(
        file_stream,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True,
    )
    file = (
        client.files()
        .create(
            body=file_metadata, media_body=media, fields="id", supportsAllDrives=True
        )
        .execute()
    )
    print(f'File ID: {file.get("id")}')
    return
