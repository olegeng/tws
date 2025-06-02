!!! READ IT BEFORE START !!!
- As it is a test task and Airflow is launched in docker-compose there are missing some
things which would be made differently in prod env, like: variables(some creds), connection
to DB, Hooks and Operators.
- Translation is commented because it is billable, and if you want u can try it with own key.
- Postgre DB for scraped data and airflow is different DBs, additionaly I launched pgAdmin with 2 cons there,
passwords is not hidden as I mentioned before, they located in pgadmin => pgpass
- I decided to scrape papers from PDF because of one reason => not every paper have HTML version.
- As it is a test task we use XCom to transfer data, but in prod env we should use S3 bucket or smth.
- This is just a test task, it wasn't supposed to be like dev or even prod version, I didn't do any configuration to
spare my time. It's supposed to run only once.


- QUICKSTART -
In order to launch:
=> docker-compose up --build

Wait for initialization and then u can access airflow by
Airflow UI	http://localhost:8080	airflow / airflow
pgAdmin	http://localhost:5050	admin@admin.com / admin

There are main page with dags, in our case u're gonna se only one:
arxiv_dag

Click on it nad push Trigger button in right top corner of screen to start first dag_run.


- GENERAL - 
Project structure:

├── dags
│   ├── arxiv_dag.py
│   └── helpers
│       ├── analytics
│       │   └── data_reports.py
│       ├── db
│       │   └── db_client.py
│       ├── pdf
│       │   └── tools.py
│       └── scrapy
│           ├── arxiv_cats_scraper.py
│           └── arxiv_papers_spider.py
├── pgadmin
│   ├── pgpass
│   └── servers.json
├── docker-compose.yaml
├── requirements.txt
└── README.md

File/Folders description:
arxiv_dag.py - our main dag
helpers - directory with functional which we can reuse, include:
    1. analytics - tools for data visualisation
    2. db - tools for working with DB.
    3. pdf - tools for pdf processing
    4. scrapy - tools for webscraping

Services we up:
- Airflow
- Airflow-init (to init evrth we need for airflow)
- Postgres for Airflow
- Postgres
- pgAdmin



- Additional info for
to see reports we generated:
docker cp <worker_id>:/opt/airflow/reports ./reports

worker_id we can see in logs of report task.
