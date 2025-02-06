from typing import Dict, Any
import itertools

class FilterModule(object):
    """Custom filter to set replication factor in kafka."""

    def filters(self):
        return {
            'set_rf': self.set_rf,
        }

    def set_rf(self, proposed_json: Dict[str, Any], new_rf: int, kafka_brokers: str) -> Dict[str, Any]:
        """Return copy of proposed_json with new replication factor"""
        updated_partitions = []
        brokers = [int(x) for x in kafka_brokers.split(',')]
        broker_cycle = itertools.cycle(brokers)

        for partition in proposed_json.get('partitions', []):
            replicas = partition['replicas']
            unique_replicas = list(set(replicas))
            length = len(unique_replicas)

            if new_rf <= length:
                new_replicas = sorted(unique_replicas, key=lambda x: brokers.index(x))[:new_rf]
            else:
                needed = new_rf - length
                possible_kafka_brokers = [broker for broker in brokers if broker not in unique_replicas]
                new_replicas = unique_replicas + possible_kafka_brokers[:needed]
            
            if new_rf == 1:
                new_replicas = [next(broker_cycle)]

            updated_partitions.append({
                'topic': partition['topic'],
                'partition': partition['partition'],
                'replicas': new_replicas
            })

        return {
            'version': proposed_json.get('version', 1),
            'partitions': updated_partitions
        }
