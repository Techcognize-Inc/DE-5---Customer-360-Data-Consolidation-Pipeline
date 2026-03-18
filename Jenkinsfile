pipeline {
  agent any
  triggers {
    githubPush()
    pollSCM('H/5 * * * *')
  }
  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }
    stage('Install dependencies') {
      steps {
        bat 'python -m pip install --upgrade pip'
        bat 'pip install -r requirements.txt'
      }
    }
    stage('Run tests') {
      steps {
        bat 'python -m pytest -q tests/test_CDC_pipeline.py tests/test_entity_resolution.py tests/test_customer360.py'
      }
    }
  }
  post {
    success {
      echo 'Build succeeded'
    }
    failure {
      echo 'Build failed'
    }
  }
}
