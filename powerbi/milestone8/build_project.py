"""Generate the six-page Import-mode Power BI project from reviewable text definitions.

Desktop is the final DAX/connector/rendering acceptance environment. This generator
never connects to Azure, queries data, or enables schedules.
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
from m8 import TABLES, ROOT, HOST, DEFAULT_BATCH, queries, window

PROJECT = ROOT/"IESO"
REPORT = PROJECT/"IESO.Report"
MODEL = PROJECT/"IESO.SemanticModel"
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"


def quoted(text):
    # M text syntax is different from JSON: double embedded quotes; use #(lf),
    # and leave Windows backslashes literal. Escaping # first preserves literal #(...).
    value=str(text).replace("#","#(#)").replace('"','""').replace("\r","#(cr)").replace("\n","#(lf)")
    return '"'+value+'"'


def measure_definitions():
    m = {}
    def add(name, expression, fmt=None, folder=None):
        item = {"name": name, "expression": expression}
        if fmt: item["formatString"] = fmt
        if folder: item["displayFolder"] = folder
        m[name] = item
    number = '#,0.00;-#,0.00;0.00'
    integer = '#,0'
    add("Selected Day", "COALESCE(SELECTEDVALUE('Calendar'[delivery_date]),MAXX(ALL('Calendar'),'Calendar'[delivery_date]))", "yyyy-mm-dd")
    add("Selected Week", "COALESCE(SELECTEDVALUE('Calendar'[week_start]),MAXX(ALL('Calendar'),'Calendar'[week_start]))", "yyyy-mm-dd")
    add("Elapsed Week Hours", "VAR w=[Selected Week] VAR marketNow=UTCNOW()-5/24 RETURN MAX(0,MIN(168,INT((marketNow-w)*24)))", integer)
    add("Data State", 'IF(COUNTROWS(ALL(MarketIntervals))=0,"NO MARKET MEASUREMENTS LOADED", "Imported observations - inspect coverage and timestamps")')
    add("Day Label", '"Selected delivery day: " & FORMAT([Selected Day],"yyyy-mm-dd") & " | IESO fixed UTC-05 reporting dates"')
    add("Week Label", '''VAR w=[Selected Week] VAR todayMarket=INT(UTCNOW()-5/24)
      VAR thisMonday=todayMarket-WEEKDAY(todayMarket,2)+1
      RETURN IF(w=thisMonday,"This week: ",IF(w<thisMonday,"Historical week: ","Future week: "))
      & FORMAT(w,"yyyy-mm-dd") & " to " & FORMAT(w+6,"yyyy-mm-dd")
      & " | elapsed complete hours: " & FORMAT([Elapsed Week Hours],"0") & "/168"''')
    add("Comparison Label", '''VAR w=[Selected Week] VAR hours=[Elapsed Week Hours]
      RETURN "Aligned periods [start, end): " & FORMAT(w,"yyyy-mm-dd HH:mm") & " to "
      & FORMAT(w+hours/24,"yyyy-mm-dd HH:mm") & " versus " & FORMAT(w-7,"yyyy-mm-dd HH:mm")
      & " to " & FORMAT(w-7+hours/24,"yyyy-mm-dd HH:mm") & " | IESO reporting clock"''')
    add("Last Covered Market UTC", 'MAXX(FILTER(ALL(MarketIntervals),NOT ISBLANK(MarketIntervals[price_cad_mwh])),MarketIntervals[interval_end_utc])', "yyyy-mm-dd HH:mm")
    add("Last Covered Demand UTC", 'MAXX(FILTER(ALL(MarketIntervals),NOT ISBLANK(MarketIntervals[demand_mw])),MarketIntervals[interval_end_utc])', "yyyy-mm-dd HH:mm")
    add("Last Covered DA UTC", 'MAXX(FILTER(ALL(PriceHourly),NOT ISBLANK(PriceHourly[day_ahead_cad_mwh])),PriceHourly[delivery_date]+PriceHourly[delivery_hour]/24+5/24)', "yyyy-mm-dd HH:mm")
    add("Last Generator Report UTC", 'MAXX(ALL(GeneratorHourly),GeneratorHourly[delivery_date]+GeneratorHourly[delivery_hour]/24+5/24)', "yyyy-mm-dd HH:mm")
    add("Last Generator Output UTC", 'MAXX(FILTER(ALL(GeneratorHourly),NOT ISBLANK(GeneratorHourly[output_mw])),GeneratorHourly[delivery_date]+GeneratorHourly[delivery_hour]/24+5/24)', "yyyy-mm-dd HH:mm")
    add("Import Refresh UTC", 'MAX(RefreshInfo[refresh_utc])', "yyyy-mm-dd HH:mm:ss")
    add("Snapshot Export UTC", 'MAX(Snapshot[exported_utc])', "yyyy-mm-dd HH:mm:ss")
    add("Snapshot State", 'SELECTEDVALUE(Snapshot[source_state],"No snapshot metadata")')
    # Selected-day metrics preserve axis filters and use the same source series throughout.
    day_stats = {
        "Day Price CAD per MWh": "AVERAGE(MarketIntervals[price_cad_mwh])",
        "Day Demand MW": "AVERAGE(MarketIntervals[demand_mw])",
        "Day Demand Peak MW": "MAX(MarketIntervals[demand_mw])",
        "Day Minimum Price": "MIN(MarketIntervals[price_cad_mwh])",
        "Day Maximum Price": "MAX(MarketIntervals[price_cad_mwh])",
        "Day Price Observations": "COUNT(MarketIntervals[price_cad_mwh])",
        "Day Demand Observations": "COUNT(MarketIntervals[demand_mw])",
    }
    for name, expression in day_stats.items():
        add(name, f"VAR d=[Selected Day] RETURN CALCULATE({expression},KEEPFILTERS(MarketIntervals[delivery_date]=d))",
            integer if "Observations" in name else number, "Selected day")
    add("Day Negative Price", '''VAR d=[Selected Day] VAR p=AVERAGE(MarketIntervals[price_cad_mwh])
      RETURN IF(SELECTEDVALUE(MarketIntervals[delivery_date])=d && p<0,p)''', number, "Selected day")
    add("Day Price Coverage", 'DIVIDE([Day Price Observations],288)', "0.0%", "Selected day")
    add("Day Demand Coverage", 'DIVIDE([Day Demand Observations],288)', "0.0%", "Selected day")
    # Weekly daily charts and weekly cards use one function: weekday filtering is explicit.
    wk = '''VAR w=[Selected Week] VAR cap=[Elapsed Week Hours]*12
      VAR dow=SELECTEDVALUE('Calendar'[weekday_order])
      VAR observations=FILTER(ALL(MarketIntervals),MarketIntervals[delivery_date]>=w
        && MarketIntervals[delivery_date]<w+7
        && (MarketIntervals[delivery_date]-w)*288+(MarketIntervals[delivery_hour]-1)*12+MarketIntervals[interval]-1<cap
        && (ISBLANK(dow)||WEEKDAY(MarketIntervals[delivery_date],2)=dow))'''
    for name, expression, fmt in [
        ("Week Daily Price", "AVERAGEX(observations,MarketIntervals[price_cad_mwh])", number),
        ("Week Observed Demand Peak MW", "MAXX(observations,MarketIntervals[demand_mw])", number),
        ("Week Price Observations", "COALESCE(COUNTX(observations,MarketIntervals[price_cad_mwh]),0)", integer),
        ("Week Demand Observations", "COALESCE(COUNTX(observations,MarketIntervals[demand_mw]),0)", integer),
        ("Week Minimum Price", "MINX(observations,MarketIntervals[price_cad_mwh])", number),
        ("Week Maximum Price", "MAXX(observations,MarketIntervals[price_cad_mwh])", number),
        ("Week Negative Intervals", "VAR valid=FILTER(observations,NOT ISBLANK(MarketIntervals[price_cad_mwh])) RETURN IF(COUNTROWS(valid)>0,COUNTROWS(FILTER(valid,MarketIntervals[price_cad_mwh]<0)))", integer),
        ("Week Price Population SD", "VAR valid=FILTER(observations,NOT ISBLANK(MarketIntervals[price_cad_mwh])) RETURN IF(COUNTROWS(valid)>=2,STDEVX.P(valid,MarketIntervals[price_cad_mwh]))", number),
    ]:
        add(name, wk+" RETURN "+expression, fmt, "Weekly summary")
    add("Week Expected Intervals", '''VAR w=[Selected Week] VAR cap=[Elapsed Week Hours]
      VAR dow=SELECTEDVALUE('Calendar'[weekday_order])
      RETURN IF(ISBLANK(dow),cap*12,MAX(0,MIN(24,cap-(dow-1)*24))*12)''', integer)
    add("Week Missing Price Intervals", '[Week Expected Intervals]-[Week Price Observations]', integer)
    add("Week Missing Demand Intervals", '[Week Expected Intervals]-[Week Demand Observations]', integer)
    # Paired comparison: only same weekday/hour/interval present on BOTH sides is eligible.
    # LOOKUPVALUE treats prior week as a seven-market-date shift in the fixed UTC-05 convention.
    for label, column, how in [("Price", "price_cad_mwh", "AVERAGEX"), ("Demand Peak", "demand_mw", "MAXX")]:
        pairs = wk+f'''
          VAR pairs=ADDCOLUMNS(observations,"PreviousValue",VAR stamp=MarketIntervals[interval_start_utc]
            RETURN LOOKUPVALUE(MarketIntervals[{column}],MarketIntervals[interval_start_utc],stamp-7))
          VAR matched=FILTER(pairs,NOT ISBLANK(MarketIntervals[{column}]) && NOT ISBLANK([PreviousValue]))'''
        for side, expr in [("Current",f"MarketIntervals[{column}]"),("Previous","[PreviousValue]")]:
            add(f"Paired {side} {label}", pairs+f" RETURN {how}(matched,{expr})", number, "Aligned comparison")
        add(f"Paired {label} Change", f"VAR a=[Paired Current {label}] VAR b=[Paired Previous {label}] RETURN IF(NOT ISBLANK(a)&&NOT ISBLANK(b),a-b)", number, "Aligned comparison")
        add(f"Paired {label} Intervals", pairs+" RETURN COALESCE(COUNTROWS(matched),0)", integer, "Aligned comparison")
        prev = wk.replace("VAR w=[Selected Week]", "VAR w=[Selected Week]-7")
        add(f"Previous {label} Observations", prev+f" RETURN COALESCE(COUNTX(observations,MarketIntervals[{column}]),0)", integer)
    # Hourly generation is averaged as power (MW). It is never summed across hours.
    fuel = '''VAR w=[Selected Week] VAR cap=[Elapsed Week Hours]
      VAR dow=SELECTEDVALUE('Calendar'[weekday_order])
      VAR observations=FILTER(FuelHourly,FuelHourly[delivery_date]>=w && FuelHourly[delivery_date]<w+7
        && (FuelHourly[delivery_date]-w)*24+FuelHourly[delivery_hour]-1<cap
        && (ISBLANK(dow)||WEEKDAY(FuelHourly[delivery_date],2)=dow))'''
    add("Week Fuel Mean Output MW", fuel+" RETURN AVERAGEX(observations,FuelHourly[output_mw])", number, "Generation")
    add("Week Fuel Hours", fuel+" RETURN COUNTX(observations,FuelHourly[output_mw])", integer, "Generation")
    add("Week Fuel Missing Output Records", fuel+" RETURN SUMX(observations,FuelHourly[reported_facilities]-FuelHourly[facilities_with_output])", integer, "Generation")
    gen = '''VAR w=[Selected Week] VAR cap=[Elapsed Week Hours]
      VAR dow=SELECTEDVALUE('Calendar'[weekday_order])
      VAR observations=FILTER(ALL(DemandGeneration),DemandGeneration[delivery_date]>=w && DemandGeneration[delivery_date]<w+7
        && (DemandGeneration[delivery_date]-w)*24+DemandGeneration[delivery_hour]-1<cap
        && (ISBLANK(dow)||WEEKDAY(DemandGeneration[delivery_date],2)=dow))
      VAR pairs=ADDCOLUMNS(observations,"PreviousOutput",VAR d=DemandGeneration[delivery_date] VAR h=DemandGeneration[delivery_hour]
        RETURN LOOKUPVALUE(DemandGeneration[reported_output_mw],DemandGeneration[delivery_date],d-7,DemandGeneration[delivery_hour],h),
        "PreviousFacilities",VAR d=DemandGeneration[delivery_date] VAR h=DemandGeneration[delivery_hour]
        RETURN LOOKUPVALUE(DemandGeneration[reported_facilities],DemandGeneration[delivery_date],d-7,DemandGeneration[delivery_hour],h),
        "PreviousMeasured",VAR d=DemandGeneration[delivery_date] VAR h=DemandGeneration[delivery_hour]
        RETURN LOOKUPVALUE(DemandGeneration[facilities_with_output],DemandGeneration[delivery_date],d-7,DemandGeneration[delivery_hour],h))
      VAR matched=FILTER(pairs,NOT ISBLANK(DemandGeneration[reported_output_mw]) && NOT ISBLANK([PreviousOutput])
        && DemandGeneration[reported_facilities]>0 && DemandGeneration[reported_facilities]=DemandGeneration[facilities_with_output]
        && [PreviousFacilities]>0 && [PreviousFacilities]=[PreviousMeasured])'''
    add("Paired Current Generation MW",gen+" RETURN AVERAGEX(matched,DemandGeneration[reported_output_mw])",number,"Aligned comparison")
    add("Paired Previous Generation MW",gen+" RETURN AVERAGEX(matched,[PreviousOutput])",number,"Aligned comparison")
    add("Paired Generation Hours",gen+" RETURN COALESCE(COUNTROWS(matched),0)",integer,"Aligned comparison")
    add("Paired Generation Change MW","VAR a=[Paired Current Generation MW] VAR b=[Paired Previous Generation MW] RETURN IF(NOT ISBLANK(a)&&NOT ISBLANK(b),a-b)",number)
    # Detail pages respect the selected Calendar date and preserve the 12-interval gates.
    for name, expression in {
        "RT Hourly CAD per MWh":"AVERAGE(PriceHourly[rt_mean_cad_mwh])",
        "DA Hourly CAD per MWh":"AVERAGE(PriceHourly[day_ahead_cad_mwh])",
        "RT minus DA CAD per MWh":"AVERAGE(PriceHourly[spread_cad_mwh])",
        "Hourly Demand MW":"AVERAGE(DemandGeneration[demand_mw])",
        "Reported Generation MW":"AVERAGE(DemandGeneration[reported_output_mw])",
    }.items():
        table="PriceHourly" if name.startswith(("RT ","DA ")) else "DemandGeneration"
        add(name,f"VAR d=[Selected Day] RETURN CALCULATE({expression},KEEPFILTERS({table}[delivery_date]=d))",number,"Hourly detail")
    for name, column in [("Facility Output MW","output_mw"),("Facility Capability MW","capability_mw"),
                         ("Facility Forecast MW","forecast_mw"),("Facility Available Capacity MW","available_capacity_mw")]:
        add(name, f"VAR d=[Selected Day] RETURN CALCULATE(SUM(GeneratorHourly[{column}]),KEEPFILTERS(GeneratorHourly[delivery_date]=d))",number,"Facility detail")
    # Report health values are not silently converted from unknown to zero.
    for name, expression, fmt in [
        ("Expected Reports","SUM(HealthRuns[expected_reports])",integer),
        ("Received Reports","SUM(HealthRuns[received_reports])",integer),
        ("Expected RT Observations","CALCULATE(SUM(HealthReports[expected_observations]),HealthReports[source] IN {\"rt_lmp\",\"rt_totals\"})",integer),
        ("Accepted RT Observations","SUM(HealthReports[accepted_unique])",integer),
        ("Rejected RT Records","SUM(HealthReports[rejected])",integer),
        ("Repeated RT Deliveries","SUM(HealthReports[repeated_deliveries])",integer),
        ("Archive Recovered Reports","IF(COUNTROWS(HealthReports)>0,COUNTROWS(FILTER(HealthReports,HealthReports[archive_recovered]=\"Yes\")))",integer),
        ("Spark Duration Seconds","MAX(HealthRuns[spark_seconds])",number),
        ("Spark Completed UTC","MAX(HealthRuns[spark_completed_utc])","yyyy-mm-dd HH:mm:ss"),
        ("Final Batch Completed UTC","MAX(HealthRuns[final_batch_completed_utc])","yyyy-mm-dd HH:mm:ss"),
    ]:
        add(name,expression,fmt,"Processing evidence")
    add("Processing Status",'SELECTEDVALUE(HealthRuns[final_batch_status],"Unknown - no final completion evidence imported")')
    add("Health Scope", '"Successful Spark/sealed-inventory evidence. Check ADF Monitor, Application Insights and Databricks for current failures. A successful job does not imply complete dates."')
    return list(m.values())


LOAD = '''(TableName as text, Sql as text, Types as list) as table =>
let
    WindowDays = Duration.Days(pEndDate-pStartDate)+1,
    Checked = if WindowDays<1 or WindowDays>14 then error "Choose 1 to 14 existing delivery dates" else Sql,
    Native = if WindowDays<1 or WindowDays>14 then error "Choose 1 to 14 existing delivery dates"
      else if pSourceMode="CSV" then
        Table.PromoteHeaders(Csv.Document(File.Contents(pSnapshotFolder & "\\" & TableName & ".csv"),
          [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),[PromoteAllScalars=true])
      else if pSourceMode="Databricks" then
        Value.NativeQuery(
          Databricks.Catalogs(pServerHostname,pHttpPath,[Catalog="ieso_dev",Database=null,Implementation="2.0"])
            {[Name="ieso_dev",Kind="Database"]}[Data], Checked, null, [EnableFolding=false])
      else error "pSourceMode must be CSV or Databricks",
    Nulls = Table.ReplaceValue(Native,"",null,Replacer.ReplaceValue,Table.ColumnNames(Native)),
    UtcDateTime = (v as any) as nullable datetime => if v=null then null
      else if Value.Is(v,type datetimezone) then DateTimeZone.RemoveZone(DateTimeZone.ToUtc(v))
      else if Value.Is(v,type datetime) then v
      else let t=Text.From(v),tail=Text.Range(t,10),zoned=Text.EndsWith(t,"Z") or Text.Contains(tail,"+") or Text.Contains(tail,"-")
        in if zoned then DateTimeZone.RemoveZone(DateTimeZone.ToUtc(DateTimeZone.FromText(t)))
           else DateTime.FromText(t,[Culture="en-US"]),
    DateTimeTypes = List.Select(Types,each _{1}=type datetime),
    Utc = Table.TransformColumns(Nulls,List.Transform(DateTimeTypes,each {_{0},UtcDateTime,type datetime})),
    Typed = Table.TransformColumnTypes(Utc,Types,"en-US"),
    Scoped = if List.Contains({"Calendar","MarketIntervals","PriceHourly","DemandGeneration","FuelHourly","GeneratorHourly","Coverage"},TableName)
      then Table.SelectRows(Typed,each [delivery_date]>=pStartDate and [delivery_date]<=pEndDate) else Typed
in Scoped'''


def column(name, kind):
    c={"name":name,"dataType":{"text":"string","int":"int64","number":"double","date":"dateTime","datetime":"dateTime"}[kind],
       "sourceColumn":name,"summarizeBy":"none"}
    if kind=="date":c.update(formatString="yyyy-mm-dd",annotations=[{"name":"UnderlyingDateTimeDataType","value":"Date"}])
    if kind=="datetime":c["formatString"]="yyyy-mm-dd HH:mm:ss"
    if kind=="number":c["formatString"]="#,0.00;-#,0.00;0.00"
    if kind=="int":c["formatString"]="#,0"
    if name=="weekday":c["sortByColumn"]="weekday_order"
    return c


def build_model(mode="CSV", folder=r"C:\ieso_project\ontario-ieso-electricity-pipeline\powerbi\milestone8\.local\data",
                start="2026-11-02",end="2026-11-15",location="CHOOSE_FROM_COVERAGE_OUTPUT",batch=DEFAULT_BATCH,warehouse=""):
    window(start,end)
    templates=queries("2000-01-03","2000-01-16","__PRICING_LOCATION__",DEFAULT_BATCH)
    params={"pSourceMode":mode,"pSnapshotFolder":folder,"pServerHostname":HOST.removeprefix("https://"),
            "pHttpPath":"/sql/1.0/warehouses/"+warehouse,"pLocation":location,"pBatchId":batch}
    expressions=[{"name":k,"kind":"m","expression":quoted(v)+' meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'} for k,v in params.items()]
    for key,value in [("pStartDate",start),("pEndDate",end)]:
        y,mo,d=map(int,value.split("-"))
        expressions.append({"name":key,"kind":"m","expression":f'#date({y},{mo},{d}) meta [IsParameterQuery=true, Type="Date", IsParameterQueryRequired=true]'})
    expressions.append({"name":"fnLoad","kind":"m","expression":LOAD})
    tables=[]
    for name,fields in TABLES.items():
        sql=templates[name].replace("2000-01-03","__START__").replace("2000-01-16","__END__").replace(DEFAULT_BATCH,"__BATCH__")
        types="{"+", ".join('{'+quoted(n)+', '+{"text":"type text","int":"Int64.Type","number":"type number","date":"type date","datetime":"type datetime"}[t]+'}' for n,t in fields)+"}"
        expr='''let
          Template = %s,
          Q1 = Text.Replace(Template,"__START__",Date.ToText(pStartDate,"yyyy-MM-dd")),
          Q2 = Text.Replace(Q1,"__END__",Date.ToText(pEndDate,"yyyy-MM-dd")),
          Q3 = Text.Replace(Q2,"__PRICING_LOCATION__",Text.Replace(pLocation,"'","''")),
          Q4 = if Text.Length(pBatchId)=24 and Text.Length(Text.Select(pBatchId,{"0".."9","a".."f"}))=24
               then Text.Replace(Q3,"__BATCH__",pBatchId) else error "Invalid M7 batch ID",
          Data = fnLoad(%s,Q4,%s)
        in Data''' % (quoted(sql),quoted(name),types)
        tables.append({"name":name,"columns":[column(n,t) for n,t in fields],
                       "partitions":[{"name":name,"mode":"import","source":{"type":"m","expression":expr}}]})
    tables += [
      {"name":"Snapshot","columns":[column(n,t) for n,t in [("exported_utc","datetime"),("source_state","text"),("start_date","date"),("end_date","date"),("reviewed_baseline_revision","text")]],
       "partitions":[{"name":"Snapshot","mode":"import","source":{"type":"m","expression":'''if pSourceMode="CSV" then
         let T=Table.PromoteHeaders(Csv.Document(File.Contents(pSnapshotFolder & "\\Snapshot.csv"),[Delimiter=",",Encoding=65001,QuoteStyle=QuoteStyle.Csv])),
         N=Table.ReplaceValue(T,"",null,Replacer.ReplaceValue,Table.ColumnNames(T)),
         D=Table.TransformColumns(N,{{"exported_utc",each DateTimeZone.RemoveZone(DateTimeZone.FromText(_)),type datetime}}),
         R=Table.TransformColumnTypes(D,{{"start_date",type date},{"end_date",type date}},"en-US") in R
         else #table(type table [exported_utc=nullable datetime,source_state=text,start_date=date,end_date=date,reviewed_baseline_revision=text],
         {{null,"DIRECT DATABRICKS IMPORT",pStartDate,pEndDate,"see HealthRuns revision"}})'''}}]},
      {"name":"RefreshInfo","isHidden":True,"columns":[column("refresh_utc","datetime")],
       "partitions":[{"name":"RefreshInfo","mode":"import","source":{"type":"m","expression":
        '#table(type table [refresh_utc=datetime], {{DateTimeZone.RemoveZone(DateTimeZone.FixedUtcNow())}})'}}]},
      {"name":"Metrics","columns":[dict(column("anchor","int"),isHidden=True)],"measures":measure_definitions(),
       "partitions":[{"name":"Metrics","mode":"import","source":{"type":"m","expression":'#table(type table [anchor=Int64.Type],{{1}})'}}]},
    ]
    relationships=[]
    for table in ("MarketIntervals","PriceHourly","DemandGeneration","FuelHourly","GeneratorHourly","Coverage"):
        relationships.append({"name":"Calendar_"+table,"fromTable":table,"fromColumn":"delivery_date",
          "toTable":"Calendar","toColumn":"delivery_date","fromCardinality":"many","toCardinality":"one","crossFilteringBehavior":"oneDirection"})
    return {"name":"IESO","compatibilityLevel":1601,"model":{"culture":"en-US","defaultPowerBIDataSourceVersion":"powerBI_V3",
      "sourceQueryCulture":"en-US","expressions":expressions,"tables":tables,"relationships":relationships,
      "annotations":[{"name":"PBI_QueryOrder","value":json.dumps([t['name'] for t in tables])},
        {"name":"IESO_M8_Definitions","value":"Import only. IESO fixed UTC-05 market dates. Source decimals unchanged; Power BI doubles reconciled at display precision."}]}}


def dump(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")


def field(table, name, measure=False):
    return {"Measure" if measure else "Column":{"Expression":{"SourceRef":{"Entity":table}},"Property":name}}


def projection(table,name,measure=False):
    return {"field":field(table,name,measure),"queryRef":f"{table}.{name}","nativeQueryRef":name}


def expr(text):return {"expr":{"Literal":{"Value":"'"+text.replace("'","''")+"'"}}}


PAGES = [
 ("market","Market Overview"), ("week","This Week"), ("compare","This Week vs Last Week"),
 ("prices","Real Time vs Day Ahead"), ("generation","Demand and Generation"), ("health","Pipeline Health")]


def build_report():
    dump(PROJECT/"IESO.pbip",{"$schema":"https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
         "version":"1.0","artifacts":[{"report":{"path":"IESO.Report"}}],"settings":{"enableAutoRecovery":True}})
    dump(REPORT/"definition.pbir",{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
          "version":"4.0","datasetReference":{"byPath":{"path":"../IESO.SemanticModel"}}})
    dump(MODEL/"definition.pbism",{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json","version":"1.0","settings":{}})
    dump(REPORT/"definition/version.json",{"$schema":SCHEMA+"versionMetadata/1.0.0/schema.json","version":"2.0.0"})
    dump(REPORT/"definition/report.json",{"$schema":SCHEMA+"report/2.0.0/schema.json",
        "themeCollection":{"baseTheme":{"name":"CY24SU06","reportVersionAtImport":"5.55","type":"SharedResources"}},
        "annotations":[{"name":"IESO_source","value":"IESO public reports. Coverage refers to reported observations, not all provincial facilities."}]})
    dump(REPORT/"definition/pages/pages.json",{"$schema":SCHEMA+"pagesMetadata/1.0.0/schema.json","pageOrder":[p[0] for p in PAGES],"activePageName":"market"})
    counts={}
    def visual(page,typ,title,x,y,w,h,roles):
        n=counts.get(page,0)+1;counts[page]=n;name=f"{page}_{n:02d}"
        state={role:{"projections":[projection(*p) for p in items]} for role,items in roles.items()}
        config={"visualType":typ,"query":{"queryState":state},"drillFilterOtherVisuals":True,
                "visualContainerObjects":{"title":[{"properties":{"show":{"expr":{"Literal":{"Value":"true"}}},"text":expr(title)}}]}}
        dump(REPORT/f"definition/pages/{page}/visuals/{name}/visual.json",{"$schema":SCHEMA+"visualContainer/2.1.0/schema.json",
          "name":name,"position":{"x":x,"y":y,"z":n,"height":h,"width":w,"tabOrder":n},"visual":config})
    def card(page,title,measure,x,y,w=270,h=90):visual(page,"card",title,x,y,w,h,{"Values":[("Metrics",measure,True)]})
    def table(page,title,items,x,y,w,h):visual(page,"tableEx",title,x,y,w,h,{"Values":items})
    def line(page,title,axis,measures,x,y,w,h):visual(page,"lineChart",title,x,y,w,h,{"Category":[axis],"Y":[("Metrics",v,True) for v in measures]})
    def slicer(page,title,table_name,name,x,y,w=290):visual(page,"slicer",title,x,y,w,85,{"Values":[(table_name,name,False)]})
    def M(name):return ("Metrics",name,True)
    for page,title in PAGES:
        dump(REPORT/f"definition/pages/{page}/page.json",{"$schema":SCHEMA+"page/2.0.0/schema.json","name":page,"displayName":title,"displayOption":"FitToPage","height":1000,"width":1440})
        heading={"week":"Weekly summary","compare":"Aligned week comparison"}.get(page,title)
        card(page,heading+" | IESO public reports","Data State",15,5,1400,55)
        if page!="health":
            slicer(page,"Choose one week" if page in {"week","compare"} else "Choose one delivery date",
                   "Calendar","week_start" if page in {"week","compare"} else "delivery_date",15,65,300)
            card(page,"Selected period", "Comparison Label" if page=="compare" else "Week Label" if page=="week" else "Day Label",330,65,1090,85)
        table(page,"Freshness | UTC timestamps; source coverage and report refresh are separate",
              [M("Last Covered Market UTC"),M("Last Covered Demand UTC"),M("Last Covered DA UTC"),M("Last Generator Report UTC"),M("Last Generator Output UTC"),M("Import Refresh UTC"),M("Snapshot Export UTC"),M("Snapshot State")],15,915,1400,75)
    for i,(name,title) in enumerate([("Day Price CAD per MWh","Mean observed-location price | CAD/MWh"),("Day Demand Peak MW","Observed 5-minute demand peak | MW"),("Day Price Coverage","Price intervals observed / 288"),("Day Demand Coverage","Demand intervals observed / 288")]):
        card("market",title,name,15+i*355,160,335)
    line("market","Five-minute observed-location average price | CAD/MWh",("MarketIntervals","interval_start_utc",False),["Day Price CAD per MWh"],15,260,700,290)
    line("market","Five-minute Ontario demand | MW",("MarketIntervals","interval_start_utc",False),["Day Demand MW"],730,260,690,290)
    table("market","Negative-price periods | blanks mean no qualifying observation",
          [("MarketIntervals","delivery_hour",False),("MarketIntervals","interval",False),M("Day Negative Price")],15,560,700,335)
    table("market","Measurement coverage | any-location price coverage is not all-location completeness",
          [("Coverage",n,False) for n in ("delivery_date","source","observed_slots","expected_slots","missing_slots","reported_entities","coverage_note")],730,560,690,335)
    for i,(name,title) in enumerate([("Week Daily Price","Interval-weighted weekly price | CAD/MWh"),("Week Observed Demand Peak MW","Observed weekly demand peak | MW"),("Week Price Population SD","Population SD of interval market averages"),("Week Negative Intervals","Negative-price intervals observed")]):card("week",title,name,15+i*355,160,335)
    line("week","Daily price pattern across this selected week",("Calendar","weekday",False),["Week Daily Price"],15,260,700,270)
    line("week","Daily observed demand peaks | not a complete-week claim",("Calendar","weekday",False),["Week Observed Demand Peak MW"],730,260,690,270)
    table("week","Reported fuel output | average MW over observed hours; no sum across time",
          [("Calendar","weekday",False),("FuelHourly","fuel_type",False),M("Week Fuel Mean Output MW"),M("Week Fuel Hours"),M("Week Fuel Missing Output Records")],15,540,700,355)
    table("week","Day-by-day availability and unusual prices",
          [("Calendar","weekday",False),M("Week Price Observations"),M("Week Demand Observations"),M("Week Expected Intervals"),M("Week Minimum Price"),M("Week Maximum Price"),M("Week Negative Intervals"),M("Week Missing Price Intervals"),M("Week Missing Demand Intervals")],730,540,690,355)
    for i,(name,title) in enumerate([("Paired Price Change","Matched price change | CAD/MWh"),("Paired Demand Peak Change","Matched demand peak change | MW"),("Paired Generation Change MW","Matched reported generation change | MW"),("Paired Price Intervals","Price intervals present on both sides")]):card("compare",title,name,15+i*355,160,335)
    line("compare","Same-weekday prices | identical observed interval pairs",("Calendar","weekday",False),["Paired Current Price","Paired Previous Price"],15,260,700,260)
    line("compare","Same-weekday observed demand peaks | matching interval pairs",("Calendar","weekday",False),["Paired Current Demand Peak","Paired Previous Demand Peak"],730,260,690,260)
    table("compare","Aligned weekday values and both-period coverage",
          [("Calendar","weekday",False),M("Paired Current Price"),M("Paired Previous Price"),M("Paired Price Change"),M("Week Price Observations"),M("Previous Price Observations"),M("Paired Price Intervals"),M("Week Expected Intervals")],15,530,1405,210)
    table("compare","Reported generation | matching hours with output for every facility in each report; facility rosters can differ",
          [("Calendar","weekday",False),M("Paired Current Generation MW"),M("Paired Previous Generation MW"),M("Paired Generation Change MW"),M("Paired Generation Hours"),M("Week Demand Observations"),M("Previous Demand Peak Observations"),M("Paired Demand Peak Intervals")],15,750,1405,145)
    slicer("prices","Imported pricing location (change pLocation to load another)","PriceHourly","pricing_location",15,160,700)
    line("prices","Hourly price comparison | RT mean requires 12 intervals",("PriceHourly","delivery_hour",False),["RT Hourly CAD per MWh","DA Hourly CAD per MWh"],15,255,1405,300)
    line("prices","RT minus DA | CAD/MWh | missing/unmatched hours stay blank",("PriceHourly","delivery_hour",False),["RT minus DA CAD per MWh"],15,565,700,330)
    table("prices","Coverage and exact hourly values",[("PriceHourly",n,False) for n in ("delivery_date","delivery_hour","pricing_location","rt_intervals","rt_mean_cad_mwh","day_ahead_cad_mwh","spread_cad_mwh")],730,565,690,330)
    slicer("generation","Generator (applies to facility detail)","GeneratorHourly","generator_name",15,160,700)
    slicer("generation","Fuel (applies to facility detail)","GeneratorHourly","fuel_type",730,160,690)
    line("generation","Ontario demand and reported facility generation | MW | distinct operational definitions",("DemandGeneration","delivery_hour",False),["Hourly Demand MW","Reported Generation MW"],15,255,1405,280)
    line("generation","Selected facilities | actual output, capability, forecast and available capacity remain separate",("GeneratorHourly","delivery_hour",False),["Facility Output MW","Facility Capability MW","Facility Forecast MW","Facility Available Capacity MW"],15,545,700,350)
    table("generation","Facility measurements | null is not zero",[("GeneratorHourly",n,False) for n in ("delivery_date","delivery_hour","generator_name","fuel_type","output_mw","capability_mw","forecast_mw","available_capacity_mw")],730,545,690,350)
    card("health","Evidence scope","Health Scope",15,65,1400,85)
    for i,(name,title) in enumerate([("Expected Reports","Reports requested in sealed inventory"),("Received Reports","Reports received in sealed inventory"),("Rejected RT Records","Rejected records | selected RT reports"),("Repeated RT Deliveries","Repeated RT deliveries | report-scoped")]):card("health",title,name,15+i*355,160,335)
    table("health","Requested processing window and separate completion evidence",
          [("HealthRuns",n,False) for n in ("batch_id","requested_start","requested_end","data_start","selected_delivery_hour","spark_passed","spark_completed_utc","final_batch_status","final_batch_completed_utc","spark_seconds")],15,260,1405,160)
    table("health","Observation reconciliation and archive recovery | null for metrics not recorded on that route",
          [("HealthReports",n,False) for n in ("source","delivery_date","delivery_hour","expected_observations","accepted_unique","rejected","repeated_deliveries","archive_recovered","without_kafka_delivery")],15,430,900,290)
    table("health","Completed stages",[("HealthSteps",n,False) for n in ("stage","completed_utc")],930,430,490,290)
    table("health","Independent data coverage | processing success does not certify a full day/week",
          [("Coverage",n,False) for n in ("delivery_date","source","observation_rows","observed_slots","expected_slots","missing_slots","coverage_note")],15,730,1405,165)


def configure(mode,folder,start,end,location,batch,warehouse):
    window(start,end)
    # Fully validate supplied non-secret values before overwriting the reviewable project.
    queries(start,end,location,batch)
    if mode=="Databricks" and not warehouse:
        raise ValueError("A SQL warehouse ID is required for direct Import.")
    fresh=build_model(mode,folder,start,end,location,batch,warehouse)
    path=MODEL/"model.bim"
    if path.exists():
        current=json.loads(path.read_text(encoding="utf-8"))
        replacements={item["name"]:item for item in fresh["model"]["expressions"] if item["name"].startswith("p")}
        current["model"]["expressions"]=[replacements.get(item["name"],item) for item in current["model"]["expressions"]]
        dump(path,current)
    else:dump(path,fresh)


def main():
    build_report()
    # Full generation is for package development. The configure command above changes
    # parameter values only, preserving measures and visual edits made in Desktop.
    dump(MODEL/"model.bim",build_model())
    # Standalone SQL is provided for review and recovery, not an automatic collection job.
    for name,sql in queries("2026-11-02","2026-11-15","CHOOSE_FROM_COVERAGE_OUTPUT",DEFAULT_BATCH).items():
        path=ROOT/"sql"/(name+".sql");path.parent.mkdir(parents=True,exist_ok=True);path.write_text(sql+";\n")
    (ROOT/"measures.dax").write_text("\n\n".join(f"// {m['name']}\n{m['name']} =\n{m['expression']}" for m in measure_definitions())+"\n")
    print("Generated six-page PBIP, Import model, SQL review copies and DAX measures. No cloud calls.")


if __name__=="__main__":main()
