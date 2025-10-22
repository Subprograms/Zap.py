from dotenv import load_dotenv
import os, sys, argparse, requests, csv, datetime, json, re, time, urllib.parse
from zoneinfo import ZoneInfo

# Always put help text on the next line (indented), never inline (uglyyyyy).
class BlockHelp(argparse.HelpFormatter):
    def _format_action(self, action):
        # option flags line
        s = "  " + self._format_action_invocation(action) + "\n"

        # help text line(s), indented
        if action.help:
            txt = self._expand_help(action)
            lines = self._split_lines(txt, self._width)
            indent = " " * (self._current_indent + 4)
            s += indent + ("\n" + indent).join(lines) + "\n"
        return s

# If you also want raw newlines in description/epilog, combine both:
class BlockHelpRaw(BlockHelp, argparse.RawDescriptionHelpFormatter):
    pass

def buildArgParser():
    sDesc = (
        "Export Zendesk tickets to CSV (and optionally XLSX).\n"
        "> Fields are loaded from a CSV/XLSX list (name -> field ID mapping).\n"
        "> Filter by date ranges (using TO / OR) and optional time window."
    )
    sExamples = (
        "Examples:\n"
        "  1) Basic export (today in Manila, whole day):\n"
        "     python Zap.py -f \"DB Zendesk Ticket Fields 20251018.csv\"\n"
        "\n"
        "  2) Date range only (whole days):\n"
        "     python Zap.py -f fields.xlsx -d \"2025-02-01 TO 2025-02-05\"\n"
        "\n"
        "  3) Date ranges with OR + time window + custom filename + XLSX:\n"
        "     python Zap.py -f fields.xlsx \\\n"
        "       -d \"2025-02-01 TO 2025-02-05 OR 2025-02-10 TO 2025-02-12\" \\\n"
        "       -s \"10:00 AM\" -e \"11:00 AM\" -o testout.csv --xlsx"
    )

    oArg = argparse.ArgumentParser(
        prog="Zap.py",
        description=sDesc,
        epilog=sExamples,
        formatter_class=BlockHelpRaw
    )

    # Required / core
    oArg.add_argument(
        "-f", "--fields", metavar="FILE", required=True,
        help="Path to CSV or XLSX file listing Zendesk field names and IDs."
    )

    # Credentials
    oArg.add_argument(
        "-c", "--creds", metavar="PATH",
        help="Path to credentials .env (default: credentials.env in script directory)."
    )

    # Filters group
    oGrpFilters = oArg.add_argument_group("Filters")
    oGrpFilters.add_argument(
        "-d", "--date", metavar="EXPR",
        help=("Date range expression using TO and OR.\n"
              "Sample: 2025-01-01 TO 2025-01-10 OR 2025-02-01 TO 2025-02-05\n"
              "If omitted: today (Asia/Manila).")
    )
    oGrpFilters.add_argument(
        "-s", "--start", metavar="TIME",
        help='Start time in HH:MM AM/PM (e.g., "10:00 AM").'
    )
    oGrpFilters.add_argument(
        "-e", "--end", metavar="TIME",
        help='End time in HH:MM AM/PM (e.g., "06:30 PM").'
    )

    # Output group
    oGrpOut = oArg.add_argument_group("Output")
    oGrpOut.add_argument(
        "-o", "--output", metavar="NAME",
        help="Output CSV filename. Default: auto-generated timestamp (yyyymmdd_hhmmss_am/pm)."
    )
    oGrpOut.add_argument(
        "--xlsx", action="store_true",
        help="Also export results to XLSX (same base filename as CSV)."
    )

    oArg.add_argument("--version", action="version", version="Zap.py 1.0")

    return oArg

def loadCreds(sCredsPath):
    if not sCredsPath:
        sScriptDir = os.path.dirname(os.path.abspath(__file__))
        sDefault = os.path.join(sScriptDir, "credentials.env")
        if not os.path.isfile(sDefault):
            print("Missing credentials.env in this folder. Create it with:")
            print("ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN")
            sys.exit(0)
        sCredsPath = sDefault
    if not os.path.isfile(sCredsPath):
        print("File not found for credentials .env")
        sys.exit(0)
    load_dotenv(dotenv_path=sCredsPath, override=True)
    sZendeskSubdomain = os.getenv("ZENDESK_SUBDOMAIN")
    sAgentEmail       = os.getenv("ZENDESK_EMAIL")
    sApiToken         = os.getenv("ZENDESK_API_TOKEN")
    if not all([sZendeskSubdomain, sAgentEmail, sApiToken]):
        print("Incomplete .env file...")
        sys.exit(0)
    return sZendeskSubdomain, sAgentEmail, sApiToken

def httpSession(tBasicAuth):
    oHttp = requests.Session()
    oHttp.auth = tBasicAuth
    oHttp.headers.update({"User-Agent": "ZenMaster/2.0", "Accept": "application/json"})
    return oHttp

def httpGetJson(oHttp, sUrl, nMaxRetries=6):
    nTry = 0
    while True:
        nTry += 1
        try:
            oResp = oHttp.get(sUrl, timeout=30)
        except requests.RequestException as e:
            if nTry >= nMaxRetries:
                print(f"Network error contacting Zendesk: {e}")
                sys.exit(1)
            time.sleep(min(2 ** (nTry - 1), 30))
            continue
        nStatus = oResp.status_code
        if nStatus == 429:
            nSleep = int(oResp.headers.get("Retry-After", "2"))
            time.sleep(nSleep)
            if nTry >= nMaxRetries:
                print("Rate limited by Zendesk too many times (429).")
                sys.exit(1)
            continue
        if 500 <= nStatus < 600:
            if nTry >= nMaxRetries:
                print(f"Zendesk server error {nStatus}.")
                sys.exit(1)
            time.sleep(min(2 ** (nTry - 1), 30))
            continue
        if nStatus in (401, 403):
            print(f"Authentication failed ({nStatus}). Check credentials.")
            sys.exit(1)
        try:
            oResp.raise_for_status()
            return oResp.json()
        except Exception as e:
            print(f"HTTP error: {e}")
            sys.exit(1)

def loadFieldsFile(sPath):
    sExt = os.path.splitext(sPath.lower())[1]
    aRows = []
    if sExt == ".xlsx":
        try:
            import pandas as pd
        except ImportError:
            print("pandas required for .xlsx field list.")
            sys.exit(1)
        df = pd.read_excel(sPath, dtype=str)
        for _, r in df.iterrows():
            aRows.append([str(r.iloc[0]).strip() if not pd.isna(r.iloc[0]) else "",
                          str(r.iloc[1]).strip() if df.shape[1] > 1 and not pd.isna(r.iloc[1]) else "",
                          str(r.iloc[2]).strip() if df.shape[1] > 2 and not pd.isna(r.iloc[2]) else ""])
    elif sExt == ".csv":
        with open(sPath, "r", encoding="utf-8-sig", newline="") as h:
            oR = csv.reader(h)
            for a in oR:
                if not a:
                    continue
                x0 = a[0].strip() if len(a) > 0 else ""
                x1 = a[1].strip() if len(a) > 1 else ""
                x2 = a[2].strip() if len(a) > 2 else ""
                aRows.append([x0, x1, x2])
    else:
        print("Unsupported field list format. Use .csv or .xlsx")
        sys.exit(1)
    aOutCols = []
    aCustomMap = {}
    print("\nFields loaded:")
    for a in aRows:
        sName = a[0]
        sId   = a[1] if a[1] else a[2]
        if not sName:
            continue
        print(f"  {sName}" + (f" [{sId}]" if sId else ""))
        aOutCols.append(sName)
        if sId:
            try:
                nId = int(float(sId))
                aCustomMap[sName] = nId
            except Exception:
                pass
    print("")
    return aOutCols, aCustomMap

def parseDateExpression(sExpr):
    if not sExpr or not sExpr.strip():
        return []
    sExprN = re.sub(r"\s+", " ", sExpr.strip())
    aParts = [p.strip() for p in sExprN.split(" OR ")]
    aRanges = []
    for p in aParts:
        if " TO " in p:
            sA, sB = [x.strip() for x in p.split(" TO ")]
            aRanges.append((sA, sB))
        else:
            aRanges.append((p, p))
    return aRanges

def parseTime12(sVal):
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*([AaPp][Mm])\s*", sVal or "")
    if not m:
        print("Invalid time, use HH:MM AM/PM.")
        sys.exit(1)
    h = int(m.group(1)); mnt = int(m.group(2)); ap = m.group(3).upper()
    if ap == "PM" and h != 12: h += 12
    if ap == "AM" and h == 12: h = 0
    return h, mnt

def timeInRange(tStart, tEnd, tX):
    if tStart <= tEnd: return tStart <= tX <= tEnd
    return tX >= tStart or tX <= tEnd

def buildFilter(aDateRanges, tTimeRange):
    def f(dT):
        sCreated = dT.get("created_at")
        if not isinstance(sCreated, str): return False
        try: oUTC = datetime.datetime.fromisoformat(sCreated.replace("Z","+00:00"))
        except: return False
        oMNL = oUTC.astimezone(ZoneInfo("Asia/Manila"))
        sD = oMNL.strftime("%Y-%m-%d"); tT = oMNL.time()
        bDate = True; bTime = True
        if aDateRanges: bDate = any(sa <= sD <= sb for (sa, sb) in aDateRanges)
        if tTimeRange: tStart, tEnd = tTimeRange; bTime = timeInRange(tStart, tEnd, tT)
        return bDate and bTime
    return f

def cellValue(v):
    if v is None: return ""
    if isinstance(v, (dict, list)): return json.dumps(v, ensure_ascii=False)
    if isinstance(v, str): return v.replace("\r"," ").replace("\n"," ")
    return v

def defaultFileStamp():
    oNow = datetime.datetime.now()
    sStamp = oNow.strftime("%Y%m%d_%I%M%S_%p")
    return sStamp[:-2] + sStamp[-2:].lower()

def main():
    oArg = argparse.ArgumentParser(
        prog="Zap.py",
        description=(
            "Zap.py - The BEST ZenDesk ticket parser\n\n"
            "Examples:\n"
            "  python Zap.py -f \"DB Zendesk Ticket Fields 20251018.csv\"\n"
            "  python Zap.py -f \"DB Zendesk Ticket Fields.xlsx\" -d \"2025-02-01 TO 2025-02-05\" --xlsx\n"
            "  python Zap.py -f fields.xlsx -d \"2025-02-01 TO 2025-02-05 OR 2025-02-10 TO 2025-02-12\" -s \"10:00 AM\" -e \"11:00 AM\" -o testout.csv --xlsx"
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )

    oArg.add_argument(
        "-c", "--creds",
        metavar="PATH",
        help="Path to credentials .env file (default: credentials.env in script directory)."
    )
    oArg.add_argument(
        "-f", "--fields",
        metavar="FILE",
        required=True,
        help="Path to CSV or XLSX file listing Zendesk field names and IDs."
    )
    oArg.add_argument(
        "-d", "--date",
        metavar="EXPR",
        help=("Date range expression (use TO and OR).\n"
              "Example: 2025-01-01 TO 2025-01-10 OR 2025-02-01 TO 2025-02-05")
    )
    oArg.add_argument(
        "-s", "--start",
        metavar="TIME",
        help="Start time in HH:MM AM/PM format (e.g., 10:00 AM)."
    )
    oArg.add_argument(
        "-e", "--end",
        metavar="TIME",
        help="End time in HH:MM AM/PM format (e.g., 06:30 PM)."
    )
    oArg.add_argument(
        "-o", "--output",
        metavar="NAME",
        help="Output CSV filename (optional). Default: auto-generated timestamp."
    )
    oArg.add_argument(
        "--xlsx",
        action="store_true",
        help="Also export results to XLSX (same base filename as CSV)."
    )
    
    oArg = buildArgParser()
    a = oArg.parse_args()
    sZendeskSubdomain, sAgentEmail, sApiToken = loadCreds(a.creds)
    sZendeskBaseUrl = f"https://{sZendeskSubdomain}.zendesk.com"
    tBasicAuth      = (f"{sAgentEmail}/token", sApiToken)
    oHttp = httpSession(tBasicAuth)

    dMe = httpGetJson(oHttp, f"{sZendeskBaseUrl}/api/v2/users/me.json")
    nMyId = dMe["user"]["id"]

    aCols, dCustomMap = loadFieldsFile(a.fields)
    aDateRanges = parseDateExpression(a.date) if a.date else []
    tTimeRange = None
    if a.start and a.end:
        h1,m1=parseTime12(a.start); h2,m2=parseTime12(a.end)
        tTimeRange=(datetime.time(h1,m1),datetime.time(h2,m2))
    elif not a.start and not a.end:
        tTimeRange=None

    fFilter = buildFilter(aDateRanges, tTimeRange)
    sOutBase = a.output if a.output else defaultFileStamp()
    sCsvOut = sOutBase if sOutBase.lower().endswith(".csv") else sOutBase + ".csv"
    nChunk = 50
    printProgress = lambda n: print(f"Parsed {n} tickets...")

    def getAll(sRole, sUrl, aSink):
        while sUrl:
            dJ=httpGetJson(oHttp,sUrl)
            for dT in dJ.get("tickets",[]):
                if fFilter(dT): aSink.append(dT)
            sUrl=dJ.get("links",{}).get("next")

    aTickets=[]
    getAll("assigned",f"{sZendeskBaseUrl}/api/v2/tickets.json?page[size]=100",aTickets)
    printProgress(len(aTickets))

    with open(sCsvOut,"w",newline="",encoding="utf-8-sig") as h:
        oW=csv.DictWriter(h,fieldnames=aCols,extrasaction="ignore",quoting=csv.QUOTE_ALL)
        oW.writeheader()
        for d in aTickets:
            oW.writerow({s: cellValue(d.get(s,"")) for s in aCols})
    print(f"Wrote {len(aTickets)} tickets -> {sCsvOut}")

    if a.xlsx:
        try:
            import xlsxwriter
        except ImportError:
            print("xlsxwriter not installed.")
            return
        sXlsx=sOutBase if sOutBase.lower().endswith(".xlsx") else sOutBase+".xlsx"
        wb=xlsxwriter.Workbook(sXlsx); ws=wb.add_worksheet("tickets")
        for j,s in enumerate(aCols): ws.write(0,j,s)
        n=1
        for d in aTickets:
            for j,s in enumerate(aCols): ws.write(n,j,d.get(s,""))
            n+=1
        wb.close()
        print(f"XLSX -> {sXlsx}")

if __name__=="__main__":
    main()
