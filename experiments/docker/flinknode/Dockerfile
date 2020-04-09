FROM openjdk:8-jre-alpine

# Prerequisites:
#   - bash -- needed because Flink's scripts are bash scripts
#   - libc6-compat -- no idea; saw it in
#       https://github.com/apache/flink/blob/master/flink-container/docker/Dockerfile
#   - sudo -- needed for editing /etc/hosts
#   - curl -- needed for accessing the Flink's REST API
#   - jq   -- needed for parsing json on command line
RUN apk add --no-cache bash libc6-compat sudo curl jq

ARG flink_dist=NOT_SET
ARG job_artifact=NOT_SET
ARG uid=NOT_SET
ARG gid=NOT_SET

ENV FLINK_HOME /opt/flink

# We create a user called flink with the uid and gid matching the outside user.
# We add the user to the wheel group and allow wheel to execute sudo without
# entering password.

RUN set -x && \
  addgroup -g $gid flink && \
  adduser -D -H -G flink -h $FLINK_HOME -u $uid flink && \
  addgroup flink wheel && \
  sed -e 's/^# \(%wheel.*NOPASSWD.*\)/\1/g' -i /etc/sudoers

ADD --chown=flink:flink $flink_dist /opt
COPY --chown=flink:flink $job_artifact /job.jar
COPY --chown=flink:flink docker/flinknode/docker-entrypoint.sh /
COPY --chown=flink:flink docker/flinknode/wait.sh /

RUN set -x && \
  ln -s /opt/flink-[0-9]* $FLINK_HOME && \
  chown -R flink:flink /opt/flink-* && \
  chown -h flink:flink $FLINK_HOME && \
  chmod +x /docker-entrypoint.sh && \
  chmod +x /wait.sh

USER flink:flink
EXPOSE 8081 6123
ENTRYPOINT [ "/docker-entrypoint.sh" ]
