/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"
	"reflect"
	"time"

	migrationv1alpha1 "github.com/kubernetes-sigs/kube-storage-version-migrator/pkg/apis/migration/v1alpha1"
	migrationclient "github.com/kubernetes-sigs/kube-storage-version-migrator/pkg/clientset"
	"github.com/kubernetes-sigs/kube-storage-version-migrator/pkg/migrator"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// KubeMigrator monitors storageVersionMigraiton objects, fulfills the
// migration, and updates the status of the storageVersionMigration objects.
type KubeMigrator struct {
	dynamic           dynamic.Interface
	migrationClient   migrationclient.Interface
	migrationInformer cache.SharedIndexInformer
}

// NewKubeMigrator create KubeMigrator.
func NewKubeMigrator(dynamic dynamic.Interface, migrationClient migrationclient.Interface) *KubeMigrator {
	informer := newStatusIndexedInformer(migrationClient)
	return &KubeMigrator{
		dynamic:           dynamic,
		migrationClient:   migrationClient,
		migrationInformer: informer,
	}
}

func (km *KubeMigrator) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	go km.migrationInformer.Run(stopCh)
	if !cache.WaitForCacheSync(stopCh, km.migrationInformer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Unable to sync caches"))
		return
	}
	wait.Until(km.process, time.Second, stopCh)
}

func (km *KubeMigrator) process() {
	// KubeMigrator has only one worker, so it doesn't need to use a
	// workqueue to ensure only one thread is processing a migration.

	// The already "Running" storageVersionMigration is the priority.
	runnings, err := km.migrationInformer.GetIndexer().ByIndex(statusIndex, statusRunning)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if len(runnings) != 0 {
		utilruntime.HandleError(km.processOne(runnings[0]))
		return
	}
	pendings, err := km.migrationInformer.GetIndexer().ByIndex(statusIndex, statusPending)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if len(pendings) != 0 {
		utilruntime.HandleError(km.processOne(pendings[0]))
		return
	}
}

func (km *KubeMigrator) processOne(obj interface{}) error {
	m, ok := obj.(*migrationv1alpha1.StorageVersionMigration)
	if !ok {
		return fmt.Errorf("expected StroageVersionMigration, got %#v", reflect.TypeOf(obj))
	}
	// get the fresh object from the apiserver to make sure the object
	// still exists, and the object is not completed.
	m, err := km.migrationClient.MigrationV1alpha1().StorageVersionMigrations(m.Namespace).Get(m.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if hasCondition(m, migrationv1alpha1.MigrationSucceeded) || hasCondition(m, migrationv1alpha1.MigrationFailed) {
		return fmt.Errorf("The migration has already completed for %v", m)
	}
	m, err = km.updateStatus(m, migrationv1alpha1.MigrationRunning, "")
	if err != nil {
		return err
	}
	core := migrator.NewMigrator(resource(m), km.dynamic)
	// If the storageVersionMigration object is deleted during Run(), Run()
	// returns with error when it tries to store the continueToken in the
	// migration object. Thus, it's not necessary to register a deletion
	// event handler that interrupts the Run().
	err = core.Run()
	utilruntime.HandleError(err)
	if err == nil {
		_, err = km.updateStatus(m, migrationv1alpha1.MigrationSucceeded, "")
		return err
	}
	_, err = km.updateStatus(m, migrationv1alpha1.MigrationFailed, err.Error())
	return err
}

// updateStatus always retry no matter what kind of error is returned by the
// apiserver, because updateStatus is the last step of the migration, it's a
// pity if the entire migration has to start over merely because status update
// failure.
func (km *KubeMigrator) updateStatus(m *migrationv1alpha1.StorageVersionMigration, condition migrationv1alpha1.MigrationConditionType, message string) (*migrationv1alpha1.StorageVersionMigration, error) {
	backoff := wait.Backoff{
		Steps:    6,
		Duration: 10 * time.Millisecond,
		Factor:   5.0,
		Jitter:   0.1,
	}
	return m, wait.ExponentialBackoff(backoff, func() (bool, error) {
		newCondition := migrationv1alpha1.MigrationCondition{
			Type:           condition,
			Status:         corev1.ConditionTrue,
			LastUpdateTime: metav1.Now(),
			Message:        message,
		}
		if i := indexOfCondition(m, condition); i != -1 {
			m.Status.Conditions[i] = newCondition
		} else {
			m.Status.Conditions = append(m.Status.Conditions, newCondition)
		}

		_, err := km.migrationClient.MigrationV1alpha1().StorageVersionMigrations(m.Namespace).UpdateStatus(m)
		if err == nil {
			return true, nil
		}
		// Always refresh and retry, no matter what kind of error is returned by the apiserver.
		updated, err := km.migrationClient.MigrationV1alpha1().StorageVersionMigrations(m.Namespace).Get(m.Name, metav1.GetOptions{})
		if err == nil {
			m = updated
		}
		return false, nil
	})
}
