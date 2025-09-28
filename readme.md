## para rodar a instancia - para instalar libs faltantes no glue
docker exec -it pa_python-glue-local-1 bash 


## para subir o docker
docker compose -f docker-compose-glue-local.yml up

## instalar o psycopg2
pip install psycopg2-binary

## subit a instancia do streamlit
streamlit run jupyter/python_pa_notebook.py   
