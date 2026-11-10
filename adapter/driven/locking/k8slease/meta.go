package k8slease

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

func metaFor(config Config) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: config.Name, Namespace: config.Namespace}
}
