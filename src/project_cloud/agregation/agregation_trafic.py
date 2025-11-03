import boto3
from boto3.dynamodb.conditions import Attr
from collections import defaultdict
from project_cloud.utils.utils_dynamodb import put_item_to_dynamodb
from datetime import datetime

def return_response_to_json(agregation_etat, agregation_zone, agregation_critiques, ingestion_timestamp_hour):
    """
    Cree les structures JSON pour les differentes agregations de trafic
    """

    # 1. Indice de Congestion Global
    total_segments = sum(agregation_etat.values())
    Indice_Congestion_Global = {
        "id": 1,
        "total_segments": total_segments,
        "nb_segments_V": agregation_etat.get('V', 0),
        "nb_segments_G": agregation_etat.get('G', 0),
        "nb_segments_O": agregation_etat.get('O', 0),
        "nb_segments_R": agregation_etat.get('R', 0),
        "nb_segments_etoile": agregation_etat.get('*', 0),
        "pct_segments_V": round((agregation_etat.get('V', 0) / total_segments) * 100, 2) if total_segments > 0 else 0,
        "pct_segments_G": round((agregation_etat.get('G', 0) / total_segments) * 100, 2) if total_segments > 0 else 0,
        "pct_segments_O": round((agregation_etat.get('O', 0) / total_segments) * 100, 2) if total_segments > 0 else 0,
        "pct_segments_R": round((agregation_etat.get('R', 0) / total_segments) * 100, 2) if total_segments > 0 else 0,
        "pct_segments_etoile": round((agregation_etat.get('*', 0) / total_segments) * 100, 2) if total_segments > 0 else 0,
        "ingestion_datetime": datetime.fromtimestamp(ingestion_timestamp_hour / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
    }

    # 2. Fluidite par Zone (Zoom)
    Fluidite_par_Zone = [
        {
            "id": index,
            "nom_zoom": nom_zoom,
            "nb_segments": data['count'],
            "vitesse_moyenne_kmh": round(data['vitesse_moyenne'], 2),
            "ingestion_datetime": datetime.fromtimestamp(ingestion_timestamp_hour / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
        }
        for index, (nom_zoom, data) in enumerate(agregation_zone.items(), start=1)
    ]

    # 3. Nombre de Segments Critiques par Etat
    Segments_Critiques = {
        "id": 1,
        "nb_segments_critiques_V": agregation_critiques.get('V', 0),
        "nb_segments_critiques_G": agregation_critiques.get('G', 0),
        "nb_segments_critiques_O": agregation_critiques.get('O', 0),
        "nb_segments_critiques_R": agregation_critiques.get('R', 0),
        "nb_segments_critiques_etoile": agregation_critiques.get('*', 0),
        "total_segments_critiques": sum(agregation_critiques.values()),
        "ingestion_datetime": datetime.fromtimestamp(ingestion_timestamp_hour / 1_000_000).strftime('%Y-%m-%d %H:%M:%S')
    }

    return Indice_Congestion_Global, Fluidite_par_Zone, Segments_Critiques


if __name__ == "__main__":
    # Initialisation table trafic DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_trafic = dynamodb.Table('traffic')

    # Seuil de vitesse critique (en km/h) - a ajuster selon vos besoins
    SEUIL_VITESSE_CRITIQUE = 30

    # Dictionnaire pour grouper par heure
    agregation_par_heure = defaultdict(lambda: {
        'agregation_etat': defaultdict(int),  # COUNT(gid) par etat
        'agregation_zone': defaultdict(lambda: {
            'total_vitesse': 0.0,
            'count': 0,
            'vitesse_moyenne': 0.0
        }),
        'agregation_critiques': defaultdict(int)  # COUNT par etat pour segments critiques
    })

    # Recuperer les donnees du jour
    today_timestamp = int(datetime.fromisoformat(datetime.today().strftime('%Y-%m-%d')).timestamp() * 1_000_000)

    scan_kwargs = {
        'FilterExpression': Attr('est_a_jour').eq(True) & Attr("ingestion_timestamp").gte(today_timestamp),
        'ProjectionExpression': "ingestion_timestamp, vitesse, nom_zoom, etat, gid"
    }


    while True:
        response = table_trafic.scan(**scan_kwargs)

        for item in response['Items']:
            ingestion_timestamp = int(item.get('ingestion_timestamp', 0))

            # Grouper par tranche de 1 heure
            timestamp_hour = int(ingestion_timestamp // 3_600_000_000) * 3_600_000_000

            etat = item.get('etat', '*')
            nom_zoom = item.get('nom_zoom', 'Inconnu')
            vitesse_str = item.get('vitesse', '')

            # 1. Agregation par etat (Indice de Congestion Global)
            agregation_par_heure[timestamp_hour]['agregation_etat'][etat] += 1

            # 2. Agregation par zone (Fluidite par Zone)
            if vitesse_str:
                try:
                    # Extraire la valeur numerique de la vitesse (format: "18 km/h")
                    vitesse_val = float(vitesse_str.split()[0])

                    agregation_par_heure[timestamp_hour]['agregation_zone'][nom_zoom]['total_vitesse'] += vitesse_val
                    agregation_par_heure[timestamp_hour]['agregation_zone'][nom_zoom]['count'] += 1

                    # 3. Segments critiques (vitesse < seuil)
                    if vitesse_val < SEUIL_VITESSE_CRITIQUE:
                        agregation_par_heure[timestamp_hour]['agregation_critiques'][etat] += 1

                except (ValueError, IndexError):
                    # Si la vitesse n'est pas parsable, on l'ignore
                    pass
            else:
                # Les segments sans vitesse (etat "*") sont consideres comme critiques
                agregation_par_heure[timestamp_hour]['agregation_critiques'][etat] += 1

        if 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break


    # Calculer les moyennes et inserer dans DynamoDB
    for timestamp_hour, data_hour in agregation_par_heure.items():
        timestamp_hour_fin = timestamp_hour + 3_600_000_000

        # Calculer les vitesses moyennes par zone
        for nom_zoom, zone_data in data_hour['agregation_zone'].items():
            if zone_data['count'] > 0:
                zone_data['vitesse_moyenne'] = zone_data['total_vitesse'] / zone_data['count']

        json_indice_congestion, json_fluidite_zone, json_segments_critiques = return_response_to_json(
            data_hour['agregation_etat'],
            data_hour['agregation_zone'],
            data_hour['agregation_critiques'],
            timestamp_hour_fin
        )

        insert_list = [
            ('aggregation_traffic_congestion_index', json_indice_congestion),
            ('aggregation_traffic_fluidity_by_zone', json_fluidite_zone),
            ('aggregation_traffic_critical_segments', json_segments_critiques)
        ]

        for table_name, data in insert_list:
            if isinstance(data, list):
                # Pour les listes (ex: Fluidite_par_Zone)
                for item in data:
                    put_item_to_dynamodb(dynamodb, table_name, item)
            else:
                # Pour les dictionnaires simples
                put_item_to_dynamodb(dynamodb, table_name, data)
