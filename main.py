# main.py
from .kafka_producer import AssetProducer
from .kafka_consumer import AssetConsumer


def kafka_produce():
    print("Generating assets individually...")
    producer = AssetProducer(topic_name="assets")

    # Generate shares
    num_shares = 5
    for _ in range(num_shares):
        share = generate_shares()
        producer.publish(share)

    # Generate bonds
    num_bonds = 3
    for _ in range(num_bonds):
        bond = generate_bonds()
        producer.publish(bond)

    # Generate commodities
    num_commodities = 2
    for _ in range(num_commodities):
        commodity = generate_commodities()
        producer.publish(commodity)

    # Generate loans
    num_loans = 4
    for _ in range(num_loans):
        loan = generate_loans()
        producer.publish(loan)

    print("All assets published to Kafka successfully!")


def start_consumers():
    print("Starting consumers...")

    # Consumers for different asset types
    share_consumer = AssetConsumer(topic_name="assets", asset_type="shares")
    bond_consumer = AssetConsumer(topic_name="assets", asset_type="bonds")
    loan_consumer = AssetConsumer(topic_name="assets", asset_type="loans")
    commodity_consumer = AssetConsumer(
        topic_name="assets", asset_type="commodities")

    # Run consumers
    share_consumer.consume()
    bond_consumer.consume()
    loan_consumer.consume()
    commodity_consumer.consume()


def main():
    # create_assets()
    kafka_produce()
    print("Waiting for messages...")
    time.sleep(10)
    start_consumers()
    return


if __name__ == "__main__":
    main()
