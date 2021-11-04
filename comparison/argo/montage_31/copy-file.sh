kubectl apply -f create-volume.yml
kubectl apply -f claim-volume.yml
kubectl apply -f pod-volume.yml

for i in $(ls ./input/)
do
    kubectl cp ./input/$i argo/task-pv-pod:/mnt/vol/
done

for i in $(ls ./bin/)
do
    kubectl cp ./bin/$i argo/task-pv-pod:/mnt/vol/
done
    
