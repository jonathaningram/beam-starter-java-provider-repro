FROM apache/beam_python3.11_sdk:2.63.0

RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

RUN python --version
RUN java -version
