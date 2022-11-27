from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['0.0.0.0:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

count = 0
while(True):
    data = {
	  "TransactionId": "12344"+str(count),
	  "TransactionDate": "2022-03-01",
	  "AccountId": "123"+str(count),
	  "TransactionAmount": "20.0",
	  "TransactionCurrencyCode": "GBP",
	  "TransactionMethod": "Internal",
	  "TransactionType": "Cr",
	  "RecipientAddressRegion": "United Kingdom",
	  "RecipientAddressPostcode": "EC1",
	  "RecipientAddressCity": "20",
	  "CurrentDateTime": "2022-03-01 10:00:00"
	}
    producer.send('transaction', value=data)
    print(count)
    print(data)
    sleep(1)
    count = count+1
