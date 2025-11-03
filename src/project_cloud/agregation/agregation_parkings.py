import boto3
from boto3.dynamodb.conditions import Attr
from collections import defaultdict
from statistics import mean as avg
from project_cloud.utils.utils_dynamodb import put_item_to_dynamodb
import uuid
from datetime import datetime

def return_response_to_json(total_places_disponibles, total_nb_places, agregation, ingestion_timestamp_hour):

    Taux_Occupation_Global = {
        "id": 1,
        "total_places_disponibles": total_places_disponibles,
        "total_nb_places": total_nb_places,
        "taux_disponibilite_pct" : round((total_places_disponibles / total_nb_places) * 100, 2) if total_nb_places > 0 else 0,
        "ingestion_datetime": datetime.fromtimestamp(timestamp_hour_fin / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')

    }

    Disponibilite_Moyenne_par_parking = [
    {
        "id": index,
        "nom_parking": nom,
        "total_places_disponibles": total_places_disponibles,
        "total_nb_places": total_nb_places,
        "pct_dispo_moyen": round((data['total_places_disponibles'] / data['total_nb_places']) * 100, 2) if data['total_nb_places'] > 0 else 0,
        "ingestion_datetime": datetime.fromtimestamp(timestamp_hour_fin / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
    }
    for index, (nom, data) in enumerate(agregation.items(), start=1)
    ]

    Tarification_de_Reference = {
        nom: {
            "id": index,
            "moy_tarif_1h": round(data['total_tarif_1h'] / data['count_tarif_1h'], 2) if data['count_tarif_1h'] > 0 else 0,
            "moy_tarif_2h": round(data['total_tarif_2h'] / data['count_tarif_2h'], 2) if data['count_tarif_2h'] > 0 else 0,
            "moy_tarif_4h": round(data['total_tarif_4h'] / data['count_tarif_4h'], 2) if data['count_tarif_4h'] > 0 else 0,
            "moy_tarif_24h": round(data['total_tarif_24h'] / data['count_tarif_24h'], 2) if data['count_tarif_24h'] > 0 else 0,
            "ingestion_datetime": datetime.fromtimestamp(timestamp_hour_fin / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
        }
        for index, (nom, data) in enumerate(agregation.items(), start=1)
    }

    Nb_Parkings_Ope = {
        "id": 1,
        "nb_parkings_ope": str(len(agregation)),
        "ingestion_datetime": datetime.fromtimestamp(timestamp_hour_fin / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
    }

    return Taux_Occupation_Global, Disponibilite_Moyenne_par_parking, Tarification_de_Reference, Nb_Parkings_Ope 


        
if __name__ == "__main__":
    # Initialisation table parkings DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_parkings = dynamodb.Table('parkings')

    # Dictionnaire pour grouper par heure
    agregation_par_heure = defaultdict(lambda: {
        'total_places_disponibles': 0,
        'total_nb_places': 0,
        'agregation': defaultdict(lambda: {
            'total_places_disponibles': 0,
            'total_nb_places': 0,

            'total_tarif_1h': 0.0,
            'count_tarif_1h': 0,

            'total_tarif_2h': 0.0,
            'count_tarif_2h': 0,

            'total_tarif_3h': 0.0,
            'count_tarif_3h': 0,

            'total_tarif_4h': 0.0,
            'count_tarif_4h': 0,

            'total_tarif_24h': 0.0,
            'count_tarif_24h': 0
        })
    })

    today_timestamp = int(datetime.fromisoformat(datetime.today().strftime('%Y-%m-%d')).timestamp() * 1_000_000)

    scan_kwargs = {
        'FilterExpression': Attr('etat').eq('ouvert') & Attr("ingestion_timestamp").gte(today_timestamp),
        'ProjectionExpression': "ingestion_timestamp, places_disponibles, nb_places, nom, tarif_1h, tarif_2h, tarif_3h, tarif_4h, tarif_24h"
    }

    tarifs_a_traiter = ['tarif_1h', 'tarif_2h', 'tarif_3h', 'tarif_4h', 'tarif_24h']

    while True:
        response = table_parkings.scan(**scan_kwargs)

        for item in response['Items']:
            ingestion_timestamp = int(item.get('ingestion_timestamp', 0))
            # Grouper par tranche de 1 heure
            timestamp_hour = int(ingestion_timestamp // 3_600_000_000) * 3_600_000_000

            places_dispo_item = int(item.get('places_disponibles', 0))
            nb_places_item = int(item.get('nb_places', 0))
            nom = item.get('nom', 'INCONNU')

            agregation_par_heure[timestamp_hour]['total_places_disponibles'] += places_dispo_item
            agregation_par_heure[timestamp_hour]['total_nb_places'] += nb_places_item

            agregation_par_heure[timestamp_hour]['agregation'][nom]['total_places_disponibles'] += places_dispo_item
            agregation_par_heure[timestamp_hour]['agregation'][nom]['total_nb_places'] += nb_places_item

            for tarif_key in tarifs_a_traiter:

                tarif_valeur = item.get(tarif_key)

                if tarif_valeur is not None:
                    try:

                        total_key = f'total_{tarif_key}'
                        count_key = f'count_{tarif_key}'

                        agregation_par_heure[timestamp_hour]['agregation'][nom][total_key] += float(tarif_valeur)
                        agregation_par_heure[timestamp_hour]['agregation'][nom][count_key] += 1

                    except (ValueError, TypeError):
                        pass

        if 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break


    for timestamp_hour, data_hour in agregation_par_heure.items():
        timestamp_hour_fin = timestamp_hour + 3_600_000_000

        json_taux_occupation_global, json_disponibilite_moyenne_par_parking, json_tarification_de_reference, json_nb_Parkings_ope = return_response_to_json(
            data_hour['total_places_disponibles'],
            data_hour['total_nb_places'],
            data_hour['agregation'],
            timestamp_hour_fin
        )

        insert_list = [
            ('aggregation_overall_occupancy_rate', json_taux_occupation_global),
            ('aggregation_average_availability_parking', json_disponibilite_moyenne_par_parking),
            ('aggregation_reference_pricing', json_tarification_de_reference),
            ('aggregation_number_of_parkings_in_operation', json_nb_Parkings_ope)
        ]

        # Insertion dans DynamoDB
        for table_name, data in insert_list:
            if isinstance(data, list):
                for item in data:
                    put_item_to_dynamodb(dynamodb, table_name, item)
            elif isinstance(data, dict) and not any(isinstance(v, dict) for v in data.values()):
                put_item_to_dynamodb(dynamodb, table_name, data)
            else:
                for key, item in data.items():
                    item_with_name = {**item, "nom_parking": key}
                    put_item_to_dynamodb(dynamodb, table_name, item_with_name)