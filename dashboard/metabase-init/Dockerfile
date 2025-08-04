FROM alpine:3.20

RUN apk add --no-cache curl jq

COPY ./init-metabase.sh /init-metabase.sh
RUN chmod +x /init-metabase.sh

CMD ["/init-metabase.sh"]