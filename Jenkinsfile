node {
  stage('Build Image &  Git changes')
  {
    checkout scm
    sh 'sleep 3m'
  }

  stage('Delete Scraper Job on GCP') {
    try {
        withKubeConfig([credentialsId: 'jenkins-routine-token-srhrz',
                    serverUrl: 'https://104.196.250.198',
                    clusterName: 'scraper-cluster-production-zonal-highcpu',
                    namespace: 'default'
                    ]) {

            sh 'kubectl delete job rappi-price-scraper-production'
            sh 'sleep 2m'
        }
    }
    catch (err) {
        withKubeConfig([credentialsId: 'jenkins-routine-token-srhrz',
                    serverUrl: 'https://104.196.250.198',
                    clusterName: 'scraper-cluster-production-zonal-highcpu',
                    namespace: 'default'
                    ]) {

            sh 'kubectl get jobs'
        }
    }
  }

  stage('Apply Scraper Job on GCP') {
    withKubeConfig([credentialsId: 'jenkins-routine-token-srhrz',
                    serverUrl: 'https://104.196.250.198',
                    clusterName: 'scraper-cluster-production-zonal-highcpu',
                    namespace: 'default'
                    ]) {
      sh 'kubectl apply -f job-prod.yaml'
      sh 'sleep 2m'
      sh 'kubectl get jobs'
    }
  }
}
