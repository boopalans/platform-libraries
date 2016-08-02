node {
    try {
        // Mark the code build 'stage'
        stage 'Build'

        def workspace = pwd() 
        def spark_home = env.SPARK_HOME
        def egg_version = env.BRANCH_NAME.replaceAll('-','_')

        sh '''
            echo $PWD
            export VERSION=$BRANCH_NAME
            echo $VERSION
            export SPARK_HOME="${SPARK_HOME}"
            SPARK_VERSION='1.5.0'
            export HADOOP_VERSION='2.6'
            cd $PWD@script;
            python setup.py bdist_egg

        '''

        stage 'Test'
        sh '''
        '''

        stage 'Deploy' 

        build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "platform-libraries"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}@script/dist/platformlibs-${egg_version}-py2.7.egg"]]

        emailext attachLog: true, body: "Build succeeded (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} succeeded", to: "${env.EMAIL_RECIPIENTS}"

    }
    catch(error) {
        emailext attachLog: true, body: "Build failed (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} failed", to: "${env.EMAIL_RECIPIENTS}"
        currentBuild.result = "FAILED"
        throw error
    }
}

