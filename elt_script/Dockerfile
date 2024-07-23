FROM python:3.8-slim

RUN apt-get update && apt-get install -y postgresql-client-15 cron

COPY start.sh /app/start.sh
# could have also been? -- COPY start.sh /app/

COPY elt_script.py credentials.py ./

WORKDIR /app

RUN echo "0 3 * * * python /app/elt_script.py" | crontab -

CMD [ "python", "elt_script.py" ]