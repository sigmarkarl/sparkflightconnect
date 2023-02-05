FROM --platform=linux/amd64 ubuntu

#COPY pyspark-3.3.1 pyspark-3.3.1

#RUN gu install python

#RUN gu install R
RUN apt update && \
    apt install -y python3 python3-pip git curl wget unzip
RUN pip install pyspark

RUN wget https://github.com/graalvm/graalvm-ce-dev-builds/releases/download/23.0.0-dev-20230203_2353/graalvm-ce-java19-linux-amd64-dev.tar.gz && \
    tar -xzf graalvm-ce-java19-linux-amd64-dev.tar.gz && \
    rm graalvm-ce-java19-linux-amd64-dev.tar.gz && \
    mv graalvm-ce-java19-23.0.0-dev /graalvm-ce-java19-23.0.0-dev

ENV PATH="/graalvm-ce-java19-23.0.0-dev/bin:${PATH}"
RUN gu install wasm

#COPY context.py /usr/local/lib/python3.10/dist-packages/pyspark/

#RUN source venv/bin/activate
#RUN ./gradlew assemble

#WORKDIR /root