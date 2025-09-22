from pytrends.request import TrendReq

def test_google_trends_connection():
    try:
        pytrends = TrendReq(hl='fr-FR', tz=360)

        kw_list = ["Test"]  
        pytrends.build_payload(kw_list, timeframe='today 1-m')

        interest = pytrends.interest_over_time()
        if interest.empty:
            print("Échec : Aucune donnée retournée par Google Trends.")
        else:
            print("Succès : Connexion à Google Trends réussie.")

    except Exception as e:
        print(f"Échec : Connexion à Google Trends échouée. Erreur : {e}")

if __name__ == '__main__':
    test_google_trends_connection()