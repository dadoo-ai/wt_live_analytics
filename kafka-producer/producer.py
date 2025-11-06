import os
import time
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
import requests

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration depuis les variables d'environnement
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MATOMO_API_URL = os.getenv('MATOMO_API_URL', 'https://matomo.worldtempus.com')
MATOMO_TOKEN = os.getenv('MATOMO_TOKEN', '01dfcb049cd32e2de8a12cf419850308')
MATOMO_SITE_ID = os.getenv('MATOMO_SITE_ID', '6')
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', '5'))
KAFKA_TOPIC = 'raw-events'


def create_kafka_producer():
    """Créer un producteur Kafka avec retry"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"✅ Connecté à Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.warning(f"⏳ Tentative {attempt + 1}/{max_retries} - Kafka non disponible: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("❌ Impossible de se connecter à Kafka après plusieurs tentatives")
                raise


def fetch_matomo_live_data():
    """
    Récupère les données live de Matomo API
    Endpoint: Live.getLastVisitsDetails
    """
    params = {
        'module': 'API',
        'method': 'Live.getLastVisitsDetails',
        'idSite': MATOMO_SITE_ID,
        'period': 'day',
        'date': 'today',
        'format': 'JSON',
        'token_auth': MATOMO_TOKEN,
        'showColumns': 'actionDetails,idVisit,visitIp,visitorId,fingerprint',
        'filter_sort_order': 'desc',
        'filter_limit': 10  # Récupère les 10 dernières visites
    }
    
    try:
        response = requests.get(MATOMO_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"📊 Récupéré {len(data)} visites depuis Matomo")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur lors de l'appel Matomo API: {e}")
        return []


def extract_action_details(visit_data):
    """
    Extrait les informations pertinentes des actionDetails
    Retourne une liste d'événements avec url, pageId et timestamp
    """
    events = []
    
    for visit in visit_data:
        if 'actionDetails' not in visit:
            continue
            
        for action in visit['actionDetails']:
            # Vérifier que c'est une action de type page view
            if action.get('type') != 'action':
                continue
                
            event = {
                'url': action.get('url', ''),
                'pageId': action.get('pageId', action.get('idpageview', '')),
                'timestamp': action.get('timestamp', int(time.time())),
                # 'serverTimePretty': action.get('serverTimePretty', ''),
                # 'pageTitle': action.get('pageTitle', ''),
                # 'timeSpent': action.get('timeSpent', 0),
                # # Métadonnées supplémentaires
                # 'visitId': visit.get('idVisit', ''),
                # 'visitorId': visit.get('visitorId', ''),
                # 'country': visit.get('country', ''),
                # 'referrerUrl': action.get('referrerUrl', ''),
            }
            
            # Ne garder que les événements avec URL valide
            if event['url']:
                events.append(event)
    
    return events


def clean_url(url):
    """
    Nettoie l'URL en supprimant les paramètres de tracking et fragments
    """
    from urllib.parse import urlparse, urlunparse
    
    parsed = urlparse(url)
    # Reconstruire l'URL sans query string ni fragment
    cleaned = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        '',  # params
        '',  # query
        ''   # fragment
    ))
    return cleaned


def send_to_kafka(producer, events):
    """
    Envoie les événements vers Kafka
    """
    sent_count = 0
    
    for event in events:
        try:
            # Nettoyer l'URL avant envoi
            event['url_cleaned'] = clean_url(event['url'])
            
            # Envoyer vers Kafka
            future = producer.send(KAFKA_TOPIC, value=event)
            # Attendre confirmation (bloquant)
            future.get(timeout=10)
            
            sent_count += 1
            logger.debug(f"📤 Envoyé: {event['url_cleaned']}")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'envoi vers Kafka: {e}")
    
    if sent_count > 0:
        logger.info(f"✅ {sent_count} événements envoyés vers Kafka topic '{KAFKA_TOPIC}'")
    
    return sent_count


def main():
    """
    Boucle principale du producer
    """
    logger.info("🚀 Démarrage du Kafka Producer pour Matomo Analytics")
    logger.info(f"📍 Matomo API: {MATOMO_API_URL}")
    logger.info(f"📍 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"⏱️  Intervalle de polling: {POLLING_INTERVAL}s")
    
    # Créer le producteur Kafka
    producer = create_kafka_producer()
    
    # Compteur pour stats
    total_events_sent = 0
    
    try:
        while True:
            logger.info(f"\n⏰ Polling Matomo API... [{datetime.now().strftime('%H:%M:%S')}]")
            
            # 1. Récupérer les données de Matomo
            visit_data = fetch_matomo_live_data()
            
            if not visit_data:
                logger.warning("⚠️  Aucune donnée reçue de Matomo")
            else:
                # 2. Extraire les actionDetails
                events = extract_action_details(visit_data)
                
                if events:
                    # 3. Envoyer vers Kafka
                    sent = send_to_kafka(producer, events)
                    total_events_sent += sent
                    logger.info(f"📊 Total envoyé depuis le démarrage: {total_events_sent}")
                else:
                    logger.info("ℹ️  Aucun événement à envoyer")
            
            # Attendre avant le prochain polling
            time.sleep(POLLING_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Arrêt demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}", exc_info=True)
    finally:
        logger.info("🔌 Fermeture du producteur Kafka")
        producer.close()
        logger.info("👋 Producer arrêté proprement")


if __name__ == '__main__':
    main()