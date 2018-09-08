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

// Package worker includes a migration worker that migrates a single resource.
package worker

import (
	"sync"

	"github.com/golang/glog"
	"github.com/openshift/origin/pkg/oc/cli/admin/migrate"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

var metadataAccessor = meta.NewAccessor()

type progressTracker interface {
	save(continueToken string) error
	load() (continueToken string, err error)
}

type worker struct {
	resource        string
	namespaceScoped bool
	client          *rest.RESTClient
	progress        progressTracker
	parallel        int
}

func (m *worker) GetRaw(namespace, name string, export bool) (runtime.Object, error) {
	req := m.client.Get().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(m.Resource).
		Name(name)
	return req.Do().Raw()
}

// TODO: try only decode the name to save memory.
func (m *worker) List(namespace, options *metav1.ListOptions) (runtime.Object, error) {
	req := m.client.Get().
		NamespaceIfScoped(metav1.NamespaceAll, m.NamespaceScoped).
		Resource(m.Resource).
		VersionedParams(options, metav1.ParameterCodec)
	return req.Do().Get()
}

func (w *worker) Run() error {
	continueToken, err := w.progress.load()
	if err != nil {
		return err
	}
	for {
		list, err := client.List(
			w.resource,
			&metav1.ListOptions{
				Limit:    500,
				Continue: continueToken,
			},
		)
		if err != nil && !errors.IsResourceExpired(err) {
			return err
		}
		var nextContinueToken string
		nextContinueToken, _ := metadataAccessor.Continue(list)
		if err != nil {
			continueToken = nextContinueToken
			continue
		}

		if err = w.batchMigrate(list); err != nil {
			return err
		}
		if len(nextContinueToken) == 0 {
			return nil
		}
		continueToken = nextContinueToken
	}
}

func (w *worker) batchMigrate(l runtime.Object) error {
	items, err := meta.ExtractList(l)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(parallel)
	work := make(chan runtime.Object)
	err := make(chan error)
	for i := 0; i < parallel; i++ {
		go func() {
			defer wg.Done()
			worklet := worklet{
				work: work,
				err:  err,
			}
			worklet.run()
		}()
	}
}

type worklet struct {
	work <-chan runtime.Object
	err  chan<- error
}

func (t *worklet) run() {
	for item := range t.work {
		retry(t.singleTry())
	}
}

func (t *worklet) singleTry() {
	r := info.Client.Get().
		Resource(info.Mapping.Resource.Resource).
		NamespaceIfScoped(info.Namespace, info.Mapping.Scope.Name() == meta.RESTScopeNameNamespace).
		Name(info.Name)
	get := r.Do()
	data, err := get.Raw()
	if err != nil {
		// since we have an error, processing the body is safe because we are not going
		// to send it back to the server.  Thus we can safely call Result.Error().
		// This is required because we want to make sure we pass an errors.APIStatus so
		// that DefaultRetriable can correctly determine if the error is safe to retry.
		return migrate.DefaultRetriable(info, get.Error())
	}

	// a nil limiter means "no limit"
	if o.limiter != nil {
		// we rate limit after performing all operations to make us less sensitive to conflicts
		// use a defer to make sure we always rate limit after a successful GET even if the PUT fails
		defer func() {
			// we have to wait until after the GET to determine how much data we will PUT
			// thus we need to double the amount to account for both operations
			// we also need to account for the initial list operation which is roughly another GET per object
			// thus we can amortize the cost of the list by adding another GET to our calculations
			// so we have 2 GETs + 1 PUT == 3 * size of data
			// this is a slight overestimate since every retry attempt will still try to account for
			// the initial list operation.  this should not be an issue since retries are not that common
			// and the rate limiting is best effort anyway.  going slightly slower is acceptable.
			latency := o.limiter.take(3 * len(data))
			// mimic rest.Request.tryThrottle logging logic
			if latency > longThrottleLatency {
				glog.V(4).Infof("Throttling request took %v, request: %s:%s", latency, "GET", r.URL().String())
			}
		}()
	}

	update := info.Client.Put().
		Resource(info.Mapping.Resource.Resource).
		NamespaceIfScoped(info.Namespace, info.Mapping.Scope.Name() == meta.RESTScopeNameNamespace).
		Name(info.Name).Body(data).
		Do()
	if err := update.Error(); err != nil {
		return migrate.DefaultRetriable(info, err)
	}

	if oldObject, err := get.Get(); err == nil {
		info.Refresh(oldObject, true)
		oldVersion := info.ResourceVersion
		if object, err := update.Get(); err == nil {
			info.Refresh(object, true)
			if info.ResourceVersion == oldVersion {
				return migrate.ErrUnchanged
			}
		} else {
			glog.V(4).Infof("unable to calculate resource version: %v", err)
		}
	} else {
		glog.V(4).Infof("unable to calculate resource version: %v", err)
	}
}
