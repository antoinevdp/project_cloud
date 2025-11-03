import boto3
from boto3.dynamodb.conditions import Attr
from collections import defaultdict
from project_cloud.utils.utils_dynamodb import put_item_to_dynamodb
from datetime import datetime

def return_response_to_json(agregation_network, agregation_destinations, agregation_par_gare, total_departs, ingestion_timestamp_hour):
    """
    Cree les structures JSON pour les differentes agregations SNCF
    """

    # 1. Nombre de departs par reseau
    Departs_par_Reseau = [
        {
            "id": index,
            "network": network,
            "nb_departs": count,
            "pct_departs": round((count / total_departs) * 100, 2) if total_departs > 0 else 0,
            "ingestion_datetime": datetime.fromtimestamp(ingestion_timestamp_hour / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
        }
        for index, (network, count) in enumerate(sorted(agregation_network.items(), key=lambda x: x[1], reverse=True), start=1)
    ]

    # 2. Top 10 destinations
    Top_Destinations = [
        {
            "id": index,
            "arrival_station": station,
            "nb_departs": count,
            "pct_departs": round((count / total_departs) * 100, 2) if total_departs > 0 else 0,
            "ingestion_datetime": datetime.fromtimestamp(ingestion_timestamp_hour / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
        }
        for index, (station, count) in enumerate(sorted(agregation_destinations.items(), key=lambda x: x[1], reverse=True)[:10], start=1)
    ]

    # 3. Nombre total de departs par gare de depart
    Total_Departs_par_Gare = [
        {
            "id": index,
            "departure_station": station,
            "nb_departs": count,
            "pct_departs": round((count / total_departs) * 100, 2) if total_departs > 0 else 0,
            "ingestion_datetime": datetime.fromtimestamp(ingestion_timestamp_hour / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
        }
        for index, (station, count) in enumerate(sorted(agregation_par_gare.items(), key=lambda x: x[1], reverse=True), start=1)
    ]

    return Departs_par_Reseau, Top_Destinations, Total_Departs_par_Gare


if __name__ == "__main__":
    # Initialisation table departures DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_departures = dynamodb.Table('departures')

    # Dictionnaire pour grouper par heure
    agregation_par_heure = defaultdict(lambda: {
        'agregation_network': defaultdict(int),      # COUNT par network
        'agregation_destinations': defaultdict(int), # COUNT par arrival_station
        'agregation_par_gare': defaultdict(int),     # COUNT par departure_station
        'total_departs': 0                           # COUNT total
    })

    # Recuperer les donnees du jour
    today_timestamp = int(datetime.fromisoformat(datetime.today().strftime('%Y-%m-%d')).timestamp() * 1_000_000)

    scan_kwargs = {
        'FilterExpression': Attr('type').eq('departures') & Attr("ingestion_timestamp").gte(today_timestamp),
        'ProjectionExpression': "ingestion_timestamp, network, arrival_station, departure_station, departure_datetime"
    }

    print("Debut du scan de la table departures...")

    while True:
        response = table_departures.scan(**scan_kwargs)

        for item in response['Items']:
            ingestion_timestamp = int(item.get('ingestion_timestamp', 0))

            # Grouper par tranche de 1 heure
            timestamp_hour = int(ingestion_timestamp // 3_600_000_000) * 3_600_000_000

            network = item.get('network', 'INCONNU')
            arrival_station = item.get('arrival_station', 'INCONNU')
            departure_station = item.get('departure_station', 'INCONNU')

            # 1. Agregation par reseau
            agregation_par_heure[timestamp_hour]['agregation_network'][network] += 1

            # 2. Agregation par destination
            agregation_par_heure[timestamp_hour]['agregation_destinations'][arrival_station] += 1

            # 3. Agregation par gare de depart
            agregation_par_heure[timestamp_hour]['agregation_par_gare'][departure_station] += 1

            # 4. Total de departs
            agregation_par_heure[timestamp_hour]['total_departs'] += 1

        if 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break

    print(f"Scan termine. {len(agregation_par_heure)} tranches horaires trouvees.")

    # Inserer dans DynamoDB
    for timestamp_hour, data_hour in agregation_par_heure.items():
        timestamp_hour_fin = timestamp_hour + 3_600_000_000

        # Generer les JSONs d'agregation
        json_departs_reseau, json_top_destinations, json_total_departs_par_gare = return_response_to_json(
            data_hour['agregation_network'],
            data_hour['agregation_destinations'],
            data_hour['agregation_par_gare'],
            data_hour['total_departs'],
            timestamp_hour_fin
        )

        insert_list = [
            ('aggregation_departures_by_network', json_departs_reseau),
            ('aggregation_departures_top_destinations', json_top_destinations),
            ('aggregation_departures_total', json_total_departs_par_gare)
        ]

        # Insertion dans DynamoDB
        print(f"Insertion des agregations pour la tranche horaire: {datetime.fromtimestamp(timestamp_hour / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')}")

        for table_name, data in insert_list:
            if isinstance(data, list):
                # Pour les listes (ex: Departs_par_Reseau, Top_Destinations)
                for item in data:
                    put_item_to_dynamodb(dynamodb, table_name, item)
            else:
                # Pour les dictionnaires simples
                put_item_to_dynamodb(dynamodb, table_name, data)

    print("Agregation SNCF terminee avec succes!")
