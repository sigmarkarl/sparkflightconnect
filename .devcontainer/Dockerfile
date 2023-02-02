FROM --platform=linux/amd64 ubuntu
COPY graalvm-ce-java11-22.3.1 graalvm-ce-java11-22.3.1
#COPY pyspark-3.3.1 pyspark-3.3.1
ENV PATH="/graalvm-ce-java11-22.3.1/bin:${PATH}"
RUN gu install wasm
#RUN gu install python
RUN gu install R
RUN apt update && \
    apt install -y python3 python3-pip
RUN pip install pyspark
COPY context.py /usr/local/lib/python3.10/dist-packages/pyspark/
#RUN source venv/bin/activate
#RUN ./gradlew assemble