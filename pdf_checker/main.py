from PyPDF2 import PdfReader
import pandas as pd
import gspread as gspread
import numpy as np
from cloudfunction_pdfchecker_support_tools import (
    get_pubs_shares,
    get_site_to_sites,
    get_referenced_dates,
)
from cloudfunction_pdfchecker_support_tools import (
    get_bq_data,
    merge_data,
    get_cmp_adunits_sheet,
    get_cmp_invoice_overall,
)
from cloudfunction_pdfchecker_support_tools import (
    invoice_linebyline_extractor,
    value_extractor,
    invoice_type,
)
from cloudfunction_pdfchecker_support_tools import (
    find_in_google_drive,
    upload_file,
    google_client,
)


def main(BFfolder="2024 06 June", version="v3"):

    terms_to_drop = ["Speld", "speld", "nin", "Nin"]
    google_drive_client = google_client()
    all_results, version_folder_id = find_in_google_drive(
        google_drive_client, BFfolder=BFfolder, version=version
    )
    filtered_results = list(
        filter(
            lambda x: all(term not in x["name"] for term in terms_to_drop), all_results
        )
    )
    invoice_billing_names = [
        item["name"].split("_")[-1].split(".")[0].lower() for item in all_results
    ]
    pdf_file_names = []
    paths_cleared = []
    for result in filtered_results:
        pdf_file_names.append(result["name"])
        paths_cleared.append(result["id"])
    pdf_file_names = tuple(pdf_file_names)
    paths_cleared = tuple(paths_cleared)
    index_to_change = invoice_billing_names.index("chathispano")
    invoice_billing_names[index_to_change] = "chathispano sl"
    index_to_change = invoice_billing_names.index("nederland")
    invoice_billing_names[index_to_change] = "nederland.fm"

    subpub_shares = get_pubs_shares()
    site_to_sites = get_site_to_sites()
    start_date, end_date = get_referenced_dates(invoice_date=BFfolder)
    df = get_bq_data(
         start_date=start_date, end_date=end_date
    )
    df_merged = merge_data(df, site_to_sites)

    df_cmp_adunits = get_cmp_adunits_sheet()
    df_cmp_invoice_overall = get_cmp_invoice_overall()
    excluded_prefixes = ["Cost_", "nin_", "WYSIWYG_"]
    included_prefixes = ["Inv_Ins_"]

    df_results = pd.DataFrame(
        columns=[
            "filename",
            "filepath",
            "version",
            "billingcode",
            "start_date_invoice",
            "end_date_invoice",
            "traded_revenue",
            "revshare_display",
            "display",
            "revshare_direct",
            "direct",
            "revshare_video",
            "video",
            "revshare_inapp",
            "inapp",
            "revshare_richmedia",
            "richmedia",
            "net_revenue",
            "compensation",
            "other_projects",
            "extra_revenue",
            "discrepancies",
            "corrections",
            "Cost_Of_Sales",
            "ad_sense_mcm",
            "massarius_commision",
            "evelyns_calculation",
            "net_total",
            "difference_evelyn_-_net_total",
            "weighted_average_commission_percentage",
            "should_have_cmp_license",
            "it_is_shown_corrently_on_the_invoice",
            "cpm_amount",
            "cpm_license",
            "invoice_type",
        ]
    )

    reference_date = BFfolder
    for pdf_file_name in pdf_file_names:
        print("PDF FILE NAME IN PROGRESS: ", pdf_file_name)
        rows = invoice_linebyline_extractor(
            google_drive_client,
            file_id=paths_cleared[pdf_file_names.index(pdf_file_name)],
            file_name=pdf_file_name,
        )
        specific_invoice_name=pdf_file_name.split('_')[-1].split('.')[0].lower()
        if specific_invoice_name=='nederland':
            specific_invoice_name=specific_invoice_name+'.fm'
        # Note: spcific_invoice_name at the beginning is the name of the invoice and right after the following lines of code
        # it gets replaced by the corresponding billingcode and then goes to value_extractor etc
        matching_rows = site_to_sites[site_to_sites["url"] == specific_invoice_name]
        if not matching_rows.empty:
            specific_invoice_name = site_to_sites[
                site_to_sites["url"] == specific_invoice_name
            ]["billingcode"].values[0]
            del matching_rows
            # Note: The mapping op must be here specific_invoice_name f.e. 'chathispano' the site_to_site.url must have it and it must return the site_to_site.billingcode
            # as specific invoice name and then it gonna run smoothly, not even the chathispano replacement is gonna be required
        if (any(pdf_file_name.startswith(prefix) for prefix in included_prefixes)) | (
            pdf_file_name[0].isdigit()
        ):
            try:
                df_temp = value_extractor(
                    df_cmp_adunits,
                    df_cmp_invoice_overall,
                    reference_date,
                    site_to_sites,
                    subpub_shares,
                    rows,
                    specific_invoice_name,
                    df_merged,
                    file_name=pdf_file_name,
                    filepath=paths_cleared[pdf_file_names.index(pdf_file_name)],
                    version=version,
                )
                df_temp["invoice_type"] = invoice_type(pdf_file_name)
                frames = [df_results, df_temp]
                df_results = pd.concat(frames)
            except Exception as e:
                print("Error file name --->", pdf_file_name, specific_invoice_name)
                print("Error --->", e)
    df_results["evelyns_calculation"] = np.around(
        df_results["evelyns_calculation"], decimals=2
    )
    df_results["difference_evelyn_-_net_total"] = np.around(
        df_results["difference_evelyn_-_net_total"], decimals=2
    )
    df_results["net_revenue"] = np.around(df_results["net_revenue"], decimals=2)

    reference_month = BFfolder.replace(" ", "")
    folder_id = version_folder_id
    upload_file(
        google_drive_client,
        folder_id,
        df=df_results,
        file_name=f"RESULTS_{version}_{reference_month}.xlsx",
    )
    upload_file(
        google_drive_client,
        folder_id,
        df=df,
        file_name=f"BQ_DATA_{version}_{reference_month}.xlsx",
    )


def handle_request(request):
    if request.method == "GET":
        # For GET requests, extract parameters from the URL
        args = request.args
        BFfolder =request.args.get('BFfolder') #"2024 08 August"
        version = request.args.get('version') #"v4"
        if BFfolder and version:
            main(BFfolder=BFfolder, version=version)
            return "OK"
        else: 
            return "NOT OK"


main()