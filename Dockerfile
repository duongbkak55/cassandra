FROM cassandra:5.0.9

LABEL org.opencontainers.image.title="Apache Cassandra 7.0 HMA (Aerospike-Inspired Hybrid Memory Architecture)" \
      org.opencontainers.image.description="Cassandra 7.0 fork with 64-byte Off-Heap Primary Index, Block Defrag, O_DIRECT, Streaming Write Buffer, Off-Heap Arena Memtable, and Fast-CAS Engine" \
      org.opencontainers.image.source="https://github.com/duongbkak55/cassandra"

RUN rm -rf /opt/cassandra/lib/*.jar /opt/cassandra/lib/x86_64 /opt/cassandra/lib/aarch64
COPY build/lib/jars/*.jar /opt/cassandra/lib/
COPY build/apache-cassandra-*.jar /opt/cassandra/lib/
COPY bin/* /opt/cassandra/bin/
COPY conf/* /etc/cassandra/
RUN rm -f /opt/cassandra/lib/byteman*.jar \
          /opt/cassandra/lib/jacoco*.jar \
          /opt/cassandra/lib/*darwin*.jar \
          /opt/cassandra/lib/*osx*.jar && \
    chmod +x /opt/cassandra/bin/* && \
    chown -R cassandra:cassandra /opt/cassandra /etc/cassandra
