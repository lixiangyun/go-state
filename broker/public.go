package broker

import (
	"encoding/json"
	"log"
)

func BrokerPublicGet(etcdconn *EtcdConn) *DataCommon {

	datacomm := new(DataCommon)

	value, err := etcdconn.Get(KEY_COMMON)
	if err != nil {
		if err == ErrIsNone {
			return datacomm
		}
		log.Fatalln(err.Error())
	}

	err = json.Unmarshal(value, datacomm)
	if err != nil {
		log.Fatalln(err.Error())
		return datacomm
	}

	return datacomm
}

func BrokerPublicPut(etcdconn *EtcdConn, cfg DataCommon) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return etcdconn.Put(KEY_COMMON, value)
}

func BrokerServerGet(etcdconn *EtcdConn) []DataBroker {

	brokerlist := make([]DataBroker, 0)

	keylist, err := etcdconn.GetAll(KEY_BROKER)
	if err != nil {
		if err == ErrIsNone {
			return brokerlist
		}
		log.Fatalln(err.Error())
	}

	for _, v := range keylist {
		var broker DataBroker
		err := json.Unmarshal([]byte(v.Value), &broker)
		if err != nil {
			log.Println(err.Error())
			continue
		}
		brokerlist = append(brokerlist, broker)
	}

	return brokerlist
}

func BrokerTopicGet(etcdconn *EtcdConn) []DataTopic {

	topiclist := make([]DataTopic, 0)

	keylist, err := etcdconn.GetAll(KEY_TOPIC)
	if err != nil {
		if err == ErrIsNone {
			return topiclist
		}
		log.Fatalln(err.Error())
	}

	for _, v := range keylist {
		var topic DataTopic
		err := json.Unmarshal([]byte(v.Value), &topic)
		if err != nil {
			log.Println(err.Error())
			continue
		}
		topiclist = append(topiclist, topic)
	}

	return topiclist
}

func BrokerTopicPut(etcdconn *EtcdConn, topic DataTopic) error {

	value, err := json.Marshal(topic)
	if err != nil {
		return err
	}

	key := KEY_TOPIC + topic.Topic

	return etcdconn.Put(key, value)
}

func BrokerPartitionPut(etcdconn *EtcdConn, partition DataPartition) error {

	value, err := json.Marshal(partition)
	if err != nil {
		return err
	}

	key := KEY_PARTITION + partition.PartitionID

	return etcdconn.Put(key, value)
}

func BrokerPartitionGet(etcdconn *EtcdConn) []DataPartition {

	partitionlist := make([]DataPartition, 0)

	keylist, err := etcdconn.GetAll(KEY_PARTITION)
	if err != nil {
		if err == ErrIsNone {
			return partitionlist
		}
		log.Fatalln(err.Error())
	}

	for _, v := range keylist {
		var partition DataPartition
		err := json.Unmarshal([]byte(v.Value), &partition)
		if err != nil {
			log.Println(err.Error(), v.Key, v.Value)
			continue
		}
		partitionlist = append(partitionlist, partition)
	}

	return partitionlist
}
