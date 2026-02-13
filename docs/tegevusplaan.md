### 2. Äriline kontekst ja strateegia: "Tallinn 2035"



Selleks, et su koodil oleks "hing", peab see lahendama mõnda Tallinna päris probleemi.



**Strateegiline eesmärk:**

Tallinna arengustrateegias "Tallinn 2035" on üks keskne siht: **"Terve Tallinn liigub"** ja **"Sõbralik linnaruum"**.



* **Probleem:** Linn tahab teada, kuidas keskkond ja elustiil mõjutavad tallinlaste tervist (nt rasvumine, hingamisteede haigused).

* **Andmevajadus:** TAI andmed haigestumuse kohta, et kõrvutada neid tulevikus nt linnaosade spordirajatiste asukoha või õhukvaliteediga (Digikaksiku teema).



**Sinu valitud kasutusjuhtum (Use Case):**



> *"Tallinna elanike tervisekäitumise ja haigestumuse dünaamika jälgimine maakondlikus lõikes, et toetada strateegilist sihti 'Terve Tallinn liigub'."*



---



### 3. Andmeallika valik (TAI PxWeb)



Lähtudes ülaltoodud strateegiast, vali TAI andmebaasist tabel, mis räägib tervisest ja mida saab filtreerida **piirkonna (Harju maakond)** järgi.



**Soovitus:** Tabel **KK10: Haigestumus diagnoosi ja maakonna järgi**.



* **Miks:** See on "kõva" statistika (mitte küsitlus), sellel on pikk ajalugu ja see võimaldab vaadata haigusi, mida linnakeskkond mõjutab (nt hingamisteede haigused).

* **Alternatiiv:** TK03 või TK15 (Tervisekäitumine), kui tahad fookust panna liikumisharrastusele.



Aga jääme **KK10** juurde, sest see on klassikaline "Fact Table" materjal.



---



### 4. Lahenduse arhitektuur (Kuidas teha "Inseneeritud" lahendus)



Siin on plaan, kuidas teha lahendus, mis on **paindlik, skaleeritav ja CLI-põhine**, vastates sinu soovile näidata taset.



#### A. Konfiguratsioon (`config.yaml`)



Ära kirjuta tabeli koode koodi sisse. Eralda need. See võimaldab "lisada allikaid" ilma koodi muutmata.



```yaml

# config.yaml

project_name: "Tallinn 2035 Terviseseire"



sources:

tai_api:

base_url: "https://statistika.tai.ee/api/v1/et/Andmebaas/02Haigestumus/01Haigestumus/KK10.px"

query_params:

Region: ["0037"] # Harju maakond (Tallinn asub siin)

Diagnosis: ["*"] # Kõik diagnoosid

Year: ["2020", "2021", "2022", "2023"] # Saab muuta dünaamiliseks koodis



warehouse:

db_path: "data/warehouse.duckdb"

schemas:

staging: "stg_haigestumus"

fact: "fct_haigestumus_trends"



logging:

level: "INFO"

file: "logs/etl_pipeline.log"



```



#### B. CLI ja "Incremental" loogika (`main.py`)



See vastab sinu soovile laadida andmeid vahemike kaupa. Kasutame `argparse`.



**Kontseptsioon:**



* `--mode full`: Tõmbab kõik aastad, teeb tabelile `TRUNCATE`.

* `--mode incremental --year 2023`: Tõmbab ainult 2023 ja teeb `MERGE` (uuendab olemasolevat, lisab uut).



#### C. Andmemudel (Star Schema Light)



Ära tee ühte lamedat tabelit. Tee kasvõi väike mudel, et näidata modelleerimisoskust (Kimball).



1. **Dimensioonid:**

* `dim_diagnoos` (Kood, Nimetus, Grupp)

* `dim_aeg` (Aasta)





2. **Faktid:**

* `fct_haigestumus` (seotud dimensioonidega ID kaudu)







---



### 5. README "Müügikõne" (Visioon)



See on koht, kus sa seod kõik kokku. Lisa README-sse peatükk **"Äriline ja tehniline visioon"**.



> **Projekti taust ja "Tallinn 2035"**

> Lahendus on loodud toetama Tallinna arengustrateegia sihti **"Terve Tallinn liigub"**. Andmeinsenerina on minu eesmärk luua usaldusväärne andmevoog, mis võimaldab linnal jälgida elanike tervisenäitajaid ja planeerida ennetustegevusi.

> **Tehniline disain ja skaleeritavus**

> Erinevalt ühekordsest *ad hoc* skriptist, on käesolev lahendus disainitud mikroteenuste arhitektuuri põhimõtetel, pidades silmas Tallinna soovi liikuda kaasaegsete andmeplatvormide (nt Microsoft Fabric) suunas:

> 1. **Metadata-driven:** Kogu ETL loogika on juhitud `config.yaml` failist. Uue TAI statistikatabeli lisamiseks ei ole vaja muuta Pythoni koodi, vaid lisada konfiguratsioon.

> 2. **Idempotentsus ja Backfill:** Lahendus toetab CLI kaudu nii ajaloolist täislaadimist (`--mode full`) kui ka inkrementaalset uuendamist (`--mode incremental --year 2024`), tagades andmete terviklikkuse ka vigade korral.

> 3. **Vaade tulevikku (Fabric/Digikaksik):** Kuigi hetkel kasutatakse lokaalset DuckDB-d (PostgreSQL asemel lihtsuse huvides), on koodimoodulid (`extractor.py`, `loader.py`) kergesti porditavad Fabric Notebookidesse või Airflow DAG-idesse.

>

>



### Kokkuvõte tegevusplaanist:



1. **Võta TAI tabel KK10.**

2. **Tee `config.yaml**`, kuhu paned URL-i ja päringu JSON body.

3. **Kirjuta `main.py**` `argparse`-iga (Full vs Incremental).

4. **Tee andmemudel:** Eralda diagnoosi tekst koodist (Dimensioon).

5. **Visualiseeri:** Streamlit graafik "Haigestumise trend Harjumaal: Hingamisteede haigused".

6. **README:** Rõhuta strateegiat ja paindlikkust.





tallinn-health-analytics/

├── data/

├── config/

├── src/

│   ├── __init__.py      <-- Teeb kausta imporditavaks

│   ├── extract.py

│   ├── load.py

│   ├── transform.py

│   ├── db_client.py

│   └── main.py          <-- Sinu CLI entrypoint

├── tests/

├── .gitignore

└── README.md



Ma pole sellega ikka rahul.

SQL koodid peavad kuskil olema.

Nii DDL, kui DML

Ning ehk võiks koodi struktuur olla veidi rohkem "convention over configuration" stiili sarnane. Mitte, et päris kausta tehes tekiks baasi automaatselt sama tabel, aga ehk võiksid koodi struktuuri kaustad kajastada näiteks andmebaasi schemasid? Kuigi noo python koodis võib olla mitu funktsiooni eks. Muidu dimensiooni laadimised ja fakti laadimised peaks kindlasti eraldama, aga need võivad olla ka ühes nn python moodulis.



Ehk on ostarbekas luua kohandatud kood mis järgib dbt põhimõtteid ehk SQL-first ja metadata-driven loogikat. Täieliku raamistiku nagu dbt või Airflow seadistamine võib olla liiast aga nende arhitektuursete mustrite matkimine demonstreerib kandidaadi kõrget tehnilist pädevust





Pentahoga oli selline struktuur:

PDI_DIR

+---dwh

|   |   .DS_Store

|   |   ._.DS_Store

|   |   dwh.kjb

|   |   INIT_dwh.kjb

|   +---d_date

|   |       INIT_d_date.kjb

|   +---d_draw

|   |       d_draw_st1.kjb

|   |       INIT_d_draw_st1.kjb

|   +---d_drawwin

|   |       d_drawwin_st1.kjb

|   |       INIT_d_drawwin_st1.kjb

|   +---d_game

|   |       INIT_d_game_st1.kjb

|   +---d_onlinetrnx

|   |       d_onlinetrnx_st1.kjb

|   |       INIT_d_onlinetrnx_st1.kjb

|   +---d_panel

|   |       d_panel_st1.kjb

|   |       INIT_d_panel_st1.kjb

|   +---d_ticket

|   |       d_ticket_st1.kjb

|   |       INIT_d_ticket_st1.kjb

|   |       testINIT_d_ticket.kjb

|   |       test_with_d_ticket_st1.kjb

|   +---f_budget

|   |       INIT_f_budget_st1.kjb

|   +---f_sales__instant_lottery

|   |       INIT_f_sales__instant_lottery_st1.kjb

|   +---f_sales__number_lottery

|   |       INIT_f_sales__number_lottery_st1.kjb

|   \---f_win

|           f_win_st1.kjb

|           INIT_f_win_st1.kjb

|

+---dwh2

|   |   INIT_dwh2.kjb

|   +---dwh2_d_country_demographics

|   |       INIT_dwh2_d_country_demographics_st1.kjb

|   +---dwh2_d_customer

|   |       INIT_dwh2_d_customer_st1.kjb

|   +---dwh2_d_customer_demographics

|   |       INIT_dwh2_d_customer_demographics_st1.kjb

|   +---dwh2_d_date

|   |       INIT_dwh2_d_date.kjb

|   +---dwh2_d_draw

|   |       INIT_dwh2_d_draw_st1.kjb

|   +---dwh2_d_game

|   |       INIT_dwh2_d_game_st1.kjb

|   +---dwh2_d_sales_channel

|   |       INIT_dwh2_d_sales_channel_st1.kjb

|   +---dwh2_d_ticket

|   |       INIT_dwh2_d_ticket_st1.kjb

|   +---dwh2_f_budget

|   |       INIT_dwh2_f_budget_st1.kjb

|   +---dwh2_f_customer_profile

|   |       INIT_dwh2_f_customer_profile_st1.kjb

|   +---dwh2_f_customer_profile_cumulative

|   |       INIT_dwh2_f_customer_profile_cumulative_st1.kjb

|   +---dwh2_f_sales_instant_lottery

|   |       INIT_dwh2_f_sales_instant_lottery_st1.kjb

|   +---dwh2_f_sales_monthly

|   |       INIT_dwh2_f_sales_monthly_st1.kjb

|   +---dwh2_f_sales_number_lottery

|   |       INIT_dwh2_f_sales_number_lottery_st1.kjb

|   +---dwh2_f_sales_number_lottery_daily

|   |       INIT_dwh2_f_sales_number_lottery_daily_st1.kjb

|   +---dwh2_t_customer_buying_frequency

|   |       INIT_dwh2_t_customer_buying_frequency_st1.kjb

|   \---dwh2_t_customer_loyalty_type

|           INIT_dwh2_t_customer_loyalty_type_st1.kjb

|

+---example_data

|       .DS_Store

|       ._.DS_Store

|       ._d_game_unit_price_hist.xlsx

|       bi_country_demographics.xls

|       bi_kiirloto_müük.xlsx

|       bi_kiirloto_müük.xlsx

|       d_game_unit_price_hist.xlsx

|       sales_items.xlsx

|

+---files

|       .gitkeep

|

+---logs

|       .gitkeep

|

+---ods

|   |   .DS_Store

|   |   ._.DS_Store

|   |   example_stat_osi_wh.kjb

|   |   stat_osi_wh.kjb

|   |   tls_lot0p1.kjb

|   |   xls.kjb

|   +---stat_osi_wh_t_hst_draw

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_t_hst_draw.kjb

|   |       INIT_stat_osi_wh_t_hst_draw_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_draw_st1_get_files.ktr

|   |       stat_osi_wh_t_hst_draw.kjb

|   |       stat_osi_wh_t_hst_draw_st1.kjb

|   |       stat_osi_wh_t_hst_draw_st1_get_files.ktr

|   +---stat_osi_wh_t_hst_drawticket

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_t_hst_drawticket.kjb

|   |       INIT_stat_osi_wh_t_hst_drawticket_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_drawticket_st1_get_files.ktr

|   |       stat_osi_wh_t_hst_drawticket.kjb

|   |       stat_osi_wh_t_hst_drawticket_st1.kjb

|   |       stat_osi_wh_t_hst_drawticket_st1_get_files.ktr

|   +---stat_osi_wh_t_hst_drawwin

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_t_hst_drawwin.kjb

|   |       INIT_stat_osi_wh_t_hst_drawwin_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_drawwin_st1_get_files.ktr

|   |       stat_osi_wh_t_hst_drawwin.kjb

|   |       stat_osi_wh_t_hst_drawwin_st1.kjb

|   |       stat_osi_wh_t_hst_drawwin_st1_get_files.ktr

|   +---stat_osi_wh_t_hst_game

|   |       INIT_stat_osi_wh_t_hst_game.kjb

|   |       INIT_stat_osi_wh_t_hst_game_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_game_st1_get_files.ktr

|   +---stat_osi_wh_t_hst_onlinetrnx

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_t_hst_onlinetrnx.kjb

|   |       INIT_stat_osi_wh_t_hst_onlinetrnx_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_onlinetrnx_st1_get_files.ktr

|   |       stat_osi_wh_t_hst_onlinetrnx.kjb

|   |       stat_osi_wh_t_hst_onlinetrnx_st1.kjb

|   |       stat_osi_wh_t_hst_onlinetrnx_st1_get_files.ktr

|   +---stat_osi_wh_t_hst_panel

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_t_hst_panel.kjb

|   |       INIT_stat_osi_wh_t_hst_panel_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_panel_st1_get_files.ktr

|   |       stat_osi_wh_t_hst_panel.kjb

|   |       stat_osi_wh_t_hst_panel_st1.kjb

|   |       stat_osi_wh_t_hst_panel_st1_get_files.ktr

|   +---stat_osi_wh_t_hst_win

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_t_hst_win.kjb

|   |       INIT_stat_osi_wh_t_hst_win_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_win_st1_get_files.ktr

|   |       stat_osi_wh_t_hst_win.kjb

|   |       stat_osi_wh_t_hst_win_st1.kjb

|   |       stat_osi_wh_t_hst_win_st1_get_files.ktr

|   +---stat_osi_wh_t_hst_winoperation

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_t_hst_winoperation.kjb

|   |       INIT_stat_osi_wh_t_hst_winoperation_st1.kjb

|   |       INIT_stat_osi_wh_t_hst_winoperation_st1_get_files.ktr

|   +---stat_osi_wh_v_etl_ticket

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_stat_osi_wh_v_etl_ticket.kjb

|   |       INIT_stat_osi_wh_v_etl_ticket_st1.kjb

|   |       INIT_stat_osi_wh_v_etl_ticket_st1_get_files.ktr

|   |       stat_osi_wh_v_etl_ticket.kjb

|   |       stat_osi_wh_v_etl_ticket_st1.kjb

|   |       stat_osi_wh_v_etl_ticket_st1_get_files.ktr

|   +---tls_lot0p1_draw

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_tls_lot0p1_draw.kjb

|   |       INIT_tls_lot0p1_draw_st1.kjb

|   |       INIT_tls_lot0p1_draw_st1_get_files.ktr

|   +---tls_lot0p1_draw_statistic

|   |       INIT_tls_lot0p1_draw_statistic.kjb

|   |       INIT_tls_lot0p1_draw_statistic_get_files.ktr

|   |       INIT_tls_lot0p1_draw_statistic_st1.kjb

|   |       INIT_tls_lot0p1_draw_statistic_st1_get_files.ktr

|   |       INIT_tls_lot0p1_tip_draw_statistic_get_files.ktr

|   +---tls_lot0p1_game_type

|   |       INIT_tls_lot0p1_game_type.kjb

|   |       INIT_tls_lot0p1_game_type_get_files.ktr

|   |       INIT_tls_lot0p1_game_type_st1.kjb

|   |       INIT_tls_lot0p1_game_type_st1_get_files.ktr

|   +---tls_lot0p1_participation

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_tls_lot0p1_participation.kjb

|   |       INIT_tls_lot0p1_participation_st1.kjb

|   |       INIT_tls_lot0p1_participation_st1_get_files.ktr

|   +---tls_lot0p1_price_list

|   |       INIT_tls_lot0p1_price_list.kjb

|   |       INIT_tls_lot0p1_price_list_get_files.ktr

|   |       INIT_tls_lot0p1_price_list_st1.kjb

|   |       INIT_tls_lot0p1_price_list_st1_get_files.ktr

|   +---tls_lot0p1_product

|   |       INIT_tls_lot0p1_product.kjb

|   |       INIT_tls_lot0p1_product_st1.kjb

|   |       INIT_tls_lot0p1_product_st1_get_files.ktr

|   |       INIT_tls_lot0p1_tip_product_get_files.ktr

|   +---tls_lot0p1_system_tip

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_tls_lot0p1_system_tip.kjb

|   |       INIT_tls_lot0p1_system_tip_st1.kjb

|   |       INIT_tls_lot0p1_system_tip_st1_get_files.ktr

|   +---tls_lot0p1_ticket

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_tls_lot0p1_ticket.kjb

|   |       INIT_tls_lot0p1_ticket_st1.kjb

|   |       INIT_tls_lot0p1_ticket_st1_get_files.ktr

|   +---tls_lot0p1_tip

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_tls_lot0p1_tip.kjb

|   |       INIT_tls_lot0p1_tip_st1.kjb

|   |       INIT_tls_lot0p1_tip_st1_get_files.ktr

|   +---tls_lot0p1_win_class

|   |       INIT_get_LOAD_ID_LIST.ktr

|   |       INIT_tls_lot0p1_win_class.kjb

|   |       INIT_tls_lot0p1_win_class_st1.kjb

|   |       INIT_tls_lot0p1_win_class_st1_get_files.ktr

|   +---xls_bi_country_demographics_sheet1

|   |       INIT_xls_bi_country_demographics_sheet1.kjb

|   |       INIT_xls_bi_country_demographics_sheet1_st1_get_files.ktr

|   +---xls_bi_eelarve_sheet1

|   |       INIT_xls_bi_eelarve_sheet1.kjb

|   |       INIT_xls_bi_eelarve_sheet1_st1_get_files.ktr

|   +---xls_bi_kiirloto_muuk_sheet1

|   |       INIT_xls_bi_kiirloto_muuk_sheet1.kjb

|   |       INIT_xls_bi_kiirloto_muuk_sheet1_st1_get_files.ktr

|   +---xls_d_game_unit_price_hist

|   |       INIT_xls_d_game_unit_price_hist_sheet1.kjb

|   |       INIT_xls_d_game_unit_price_hist_sheet1_st1_get_files.ktr

|   \---xls_sales_items_sheet1

|           INIT_xls_sales_items_sheet1.kjb

|           INIT_xls_sales_items_sheet1_st1_get_files.ktr

|

+---schedule

|       dwh__dwh.sh

|       INIT_dwh2_dwh2.sh

|       ods__stat_osi_wh.sh

|       ods__stat_osi_wh_AND_ods__xls_AND_dwh__dwh.sh

|       ods__xls.sh

|       test.sh

|

\---technical

        get_CURRENT_LOADED_TS.ktr

        get_JOB_VARIABLES _test.ktr

        get_JOB_VARIABLES.ktr

        get_LAST_LOADED_TS.ktr

        get_LOAD_ID_LIST.ktr

        get_LOAD_ID_LIST_DRAW.ktr

        get_LOAD_MONTHS_LIST.ktr

        get_LOAD_YEARS_LIST.ktr

        get_LOG_KEY.ktr

        obfuscate_deobfuscate_string.ktr





Ning kui näiteks ETL teha, siis peaks ilmselt algul transformeerima dimensioonid staging kihti, siis laadima näiteks dwh kihiti, kui vaja siis ajalooga vms, aga ilmselt pole vaja. Siis transformeerima minu meelest faktid staging kihti valmis ja siis laadima faktid dwh kihti. Surrogaatvõtmed jne õiges kohas tuleks luua.



Aga uuri dbt, airflow vms elt/elt mustreid, et kuidas kasutad luua või andmeaida kihid luua. Mulle väga see pronks, silver ja gold ei meeldi, staging, ods, dwh vms tundub nagu arusaadavam. Aga uuri kuidas koostada python stuktuur, kaustad, moodulid/failid, et järgiksid tänapäevaseid standardeid või oleksid intuitiivsed. Uuri ka kas saan päris ilma Python ETL frameworks kasutamata või oleks siiski mingi lightweight vms mõistlik?

