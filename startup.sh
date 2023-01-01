DEFAULT_PROFILE=""
if [ -z "$1" ]
then
   DEFAULT_PROFILE="cubo-network"
fi
PROFILE_NAME=$DEFAULT_PROFILE
WORKSPACE_LOCATION=$PWD
SCRIPT_FILE_NAME=./src/main.py
docker run -it -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pyspark amazon/aws-glue-libs:glue_libs_3.0.0_image_01 pyspark