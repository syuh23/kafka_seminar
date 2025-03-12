# ✨ Spring Kafka Seminar 🍀

Spring Kafka의 동작 과정과 초기화 과정을 공부함.


## 🔥 진행 중

☑️ Spring에서 Consumer Profile을 종료한 후 재실행했을 때에도 Kafka 서버에 저장된 Consumer와 List가 Spring Profile에서도 존재해야 함


## 🔥 완료

✅ Spring Kafka가 실행되면서 Producer와 Consumer를 생성하는 과정

✅ Spring Profile 기능을 활용하여 Producer Profile에서는 Producer만 생성되고, Consumer Profile에서는 Consumer만 실행되게 코드 수정

✅ API 요청이 들어왔을 떄 Consumer가 동적으로 생성되게 코드 수정

- 하나의 서버에 여러 개의 Consumer가 동적으로 생성되기 때문에 각각을 구별할 무언가가 필요.
- 각 Consumer는 자신이 구독하고 있는 Topic으로 들어온 메시지를 읽고, 이를 각각의 List에 저장.
- 해당 List 출력 시 Consumer Name을 입력받음. 입력받은 Name을 가진 Consumer가 지금까지 읽었던 메시지를 전부 출력.

---
