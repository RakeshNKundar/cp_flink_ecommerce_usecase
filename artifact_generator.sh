docker rmi telefonica-flink-demo:26.0

docker buildx build --platform linux/amd64,linux/arm64 -t telefonica-flink-demo:27.0 .

docker tag telefonica-flink-demo:27.0 17090974127/telefonica-flink-demo:27.0

docker push 17090974127/telefonica-flink-demo:27.0