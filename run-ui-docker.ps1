docker build -t cs2-predictor-ui ./cs2-predictor-ui

docker run -p 3000:3000 --dns 8.8.8.8 `
    -e KAFKA_BROKERS=d85nitghvfrjvm53g2ug.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092 `
    -e KAFKA_SASL_MECHANISM=scram-sha-256 `
    -e KAFKA_SASL_USERNAME=cs2-brocker `
    -e KAFKA_SASL_PASSWORD=sKUVi0gQajnd2rcPrUGacQ5A5MX8Wa `
    cs2-predictor-ui
