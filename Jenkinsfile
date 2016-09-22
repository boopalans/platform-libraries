node {
    try {
        // Mark the code build 'stage'
        stage 'Build'

        deleteDir()
        
        checkout scm
        
        def workspace = pwd()
        def version = env.BRANCH_NAME
        def spark_home = env.SPARK_HOME
        
        if(env.BRANCH_NAME=="master") {
            version = sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
            checkout([$class: 'GitSCM', branches: [[name: "tags/${version}"]], extensions: [[$class: 'CleanCheckout']]])
        }

        def egg_version = version.replaceAll('-','_')

        sh """
            export VERSION=${version}
            export SPARK_HOME="${spark_home}"
            SPARK_VERSION='1.5.0'
            export HADOOP_VERSION='2.6'
            python setup.py bdist_egg
        """

        stage 'Test'
        sh '''
        '''

        stage 'Deploy' 

        build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "platform-libraries"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}/dist/platformlibs-${egg_version}-py2.7.egg"]]

        emailext attachLog: true, body: "Build succeeded (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} succeeded", to: "${env.EMAIL_RECIPIENTS}"

    }
    catch(error) {
        emailext attachLog: true, body: "Build failed (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} failed", to: "${env.EMAIL_RECIPIENTS}"
        currentBuild.result = "FAILED"
        throw error
    }
}

