import math
from flask import resquest 


# Url de API des données en temps réelles 
url = "https://opendata.reseaux-energies.fr/api/records/1.0/search/?dataset=eco2mix-national-tr&q=&sort=-date_heure&rows=1"
response = requests.get(url)
# Juste pour récupe la date
url_date = "https://opendata.reseaux-energies.fr/api/records/1.0/search/?dataset=eco2mix-national-tr&q=&sort=-date_heure&rows=1"
resp_date = requests.get(url)
resp_date.raise_for_status()
data = resp_date.json()


# ============ Récupération des données de lAPI ============================================

Boutton = request.form.get("Value")
if Boutton == 1 or 2 or 3 or 4 or 5 :
    if response.status_code == 200:
        # récupération de toutes les données de l'API
        data = response.json()
        record = data["records"][0]["fields"]

        # ======= Réccupération puis stokage de chaque type de données 

        # récupération de la date et heure des données 
        record_date = data["records"][0]
        date_heure = record_date.get("record_timestamp")

        # récupération des données de hydrolique + Somme 
        Hydraulique = record["hydraulique"]
        Hydraulique_lacs = record["hydraulique_lacs"] 
        hydraulique_fil_eau_eclusee = record["hydraulique_fil_eau_eclusee"]
        hydraulique_step_turbinage = record["hydraulique_step_turbinage"]
        SommeHydraulique = Hydraulique + Hydraulique_lacs + hydraulique_fil_eau_eclusee + hydraulique_step_turbinage

        # Récuparation des données du Solaire 
        Solaire = record["solaire"]
    
        # Récupération des données du l'éolien 
        eolien_terrestre = record["eolien_terrestre"]
        eolien_offshore = record["eolien_offshore"]
        eolien = record["eolien"]
        SommeEolien = eolien_terrestre + eolien_offshore + eolien

        # Récupération des données du charbon 
        charbon = record["charbon"]

        # Récupération des données du fioul 
        fioul_autres = record["fioul_autres"]
        fioul_tac = record["fioul_tac"]
        fioul_cogen = record["fioul_cogen"]
        SommeFioul = fioul_autres + fioul_cogen + fioul_tac

        # Récupération des données du bioenergies 
        bioenergies = record["bioenergies_dechets"]
        bioenergies_biogaz = record["bioenergies_biogaz"]
        bioenergies_biomasse = record["bioenergies_biomasse"]
        bioenergies = record["bioenergies"]
        bioenergies_dechets = record["bioenergies_dechets"]
        SommeBioenergies = bioenergies + bioenergies_biomasse + bioenergies_biogaz + bioenergies + bioenergies_dechets

        # Récupération des données du gaz 
        gaz_cogen = record["gaz_cogen"]
        gaz = record["gaz"]
        gaz_autres = record["gaz_autres"]
        gaz_tac = record["gaz_tac"]
        gaz_ccg = record["gaz_ccg"]
        SommeGaz = gaz_ccg + gaz_tac + gaz_autres + gaz + gaz_cogen 

        # Récupération des données du nucleaire
        nucleaire = record["nucleaire"]

        #import/export
        Signeanglais = record["ech_comm_angleterre"]
        anglais = math.sqrt(Signeanglais**2)

        Signeallemagne = record["ech_comm_allemagne_belgique"]
        allemagne = math.sqrt(Signeallemagne**2)

        Signeespagne = record["ech_comm_espagne"]
        espagne = math.sqrt(Signeespagne**2)

        Signeitalie = record["ech_comm_italie"]
        italie = math.sqrt(Signeitalie**2)
    

        # Somme total de la production Français 
        ProductionTotal = nucleaire + SommeGaz + SommeBioenergies + SommeFioul + charbon + SommeEolien + Solaire + SommeHydraulique

        nucleaire100 = nucleaire / ProductionTotal * 100
        Solaire100 = Solaire / ProductionTotal * 100
        eolien100 = eolien / ProductionTotal * 100
        Hydraulique100 = SommeHydraulique / ProductionTotal * 100
        charbon100 = charbon / ProductionTotal * 100


    else :
        print("Erreur de connexion : ", response.status_code)

