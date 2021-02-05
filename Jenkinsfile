node {
  stage('Build Image &  Git changes')
  {
    checkout scm
    sh 'sleep 3m'
  }

  stage('Delete Scraper Job on IBM') {
    try {
        withKubeConfig([credentialsId: 'jenkins-token-8vlqc',
                    serverUrl: 'https://c7.us-south.containers.cloud.ibm.com:23417',
                    clusterName: 'scraper-mex-prod',
                    namespace: 'default'
                    ]) {

            sh 'kubectl delete job rappi-price-scraper-production'
            sh 'sleep 2m'
        }
    }
    catch (err) {
        withKubeConfig([credentialsId: 'jenkins-token-8vlqc',
                    serverUrl: 'https://c7.us-south.containers.cloud.ibm.com:23417',
                    clusterName: 'scraper-mex-prod',
                    namespace: 'default'
                    ]) {

            sh 'kubectl get jobs'
        }
    }
  }

  stage('Apply Scraper Job on GCP') {
    withKubeConfig([credentialsId: 'jenkins-token-8vlqc',
                    serverUrl: 'https://c7.us-south.containers.cloud.ibm.com:23417',
                    clusterName: 'scraper-mex-prod',
                    namespace: 'default'
                    ]) {
      sh 'kubectl apply -f job-prod.yaml'
      sh 'sleep 2m'
      sh 'kubectl get jobs'
    }
  }
}
