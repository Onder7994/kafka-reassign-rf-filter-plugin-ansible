# kafka-reassign-rf-filter-plugin-ansible

Usage to reassign replicaton factor in kafka with ansible

Example

```yaml
- name: Modify proposed json via custom filter
  set_fact:
    updated_proposed_json: "{{ proposed_json | set_rf(NEW_RF|int, kafka_broker_list) }}"
  when:
    - proposed_json is defined
```