# A static binary and nothing else; spill files go under /tmp, which the container provides.
FROM scratch
COPY bin/wcol-reducer /wcol-reducer
ENV PORT=8080 SPILL_DIR=/tmp/wcol-spill
EXPOSE 8080
CMD ["/wcol-reducer"]
