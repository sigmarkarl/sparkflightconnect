FROM --platform=linux/amd64 ubuntu
COPY graalvm-ce-java19-23.0.0-dev graalvm-ce-java19-23.0.0-dev

#COPY pyspark-3.3.1 pyspark-3.3.1

ENV PATH="/graalvm-ce-java19-23.0.0-dev/bin:${PATH}"
RUN gu install wasm

#RUN gu install python

#RUN gu install R
RUN apt update && \
    apt install -y python3 python3-pip git
RUN pip install pyspark
COPY context.py /usr/local/lib/python3.10/dist-packages/pyspark/

#RUN source venv/bin/activate
#RUN ./gradlew assemble

#WORKDIR /root