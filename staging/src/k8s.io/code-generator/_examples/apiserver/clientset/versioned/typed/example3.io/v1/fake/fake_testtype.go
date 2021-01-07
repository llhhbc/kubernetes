/*
Copyright The Kubernetes Authors.

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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
	example3iov1 "k8s.io/code-generator/_examples/apiserver/apis/example3.io/v1"
)

// FakeTestTypes implements TestTypeInterface
type FakeTestTypes struct {
	Fake *FakeThirdExampleV1
	ns   string
}

var testtypesResource = schema.GroupVersionResource{Group: "example.dots.apiserver.code-generator.k8s.io", Version: "v1", Resource: "testtypes"}

var testtypesKind = schema.GroupVersionKind{Group: "example.dots.apiserver.code-generator.k8s.io", Version: "v1", Kind: "TestType"}

// Get takes name of the testType, and returns the corresponding testType object, and an error if there is any.
func (c *FakeTestTypes) Get(ctx context.Context, name string, options v1.GetOptions) (result *example3iov1.TestType, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(testtypesResource, c.ns, name), &example3iov1.TestType{})

	if obj == nil {
		return nil, err
	}
	return obj.(*example3iov1.TestType), err
}

// List takes label and field selectors, and returns the list of TestTypes that match those selectors.
func (c *FakeTestTypes) List(ctx context.Context, opts v1.ListOptions) (result *example3iov1.TestTypeList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(testtypesResource, testtypesKind, c.ns, opts), &example3iov1.TestTypeList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &example3iov1.TestTypeList{ListMeta: obj.(*example3iov1.TestTypeList).ListMeta}
	for _, item := range obj.(*example3iov1.TestTypeList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested testTypes.
func (c *FakeTestTypes) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(testtypesResource, c.ns, opts))

}

// Create takes the representation of a testType and creates it.  Returns the server's representation of the testType, and an error, if there is any.
func (c *FakeTestTypes) Create(ctx context.Context, testType *example3iov1.TestType, opts v1.CreateOptions) (result *example3iov1.TestType, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(testtypesResource, c.ns, testType), &example3iov1.TestType{})

	if obj == nil {
		return nil, err
	}
	return obj.(*example3iov1.TestType), err
}

// Update takes the representation of a testType and updates it. Returns the server's representation of the testType, and an error, if there is any.
func (c *FakeTestTypes) Update(ctx context.Context, testType *example3iov1.TestType, opts v1.UpdateOptions) (result *example3iov1.TestType, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(testtypesResource, c.ns, testType), &example3iov1.TestType{})

	if obj == nil {
		return nil, err
	}
	return obj.(*example3iov1.TestType), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeTestTypes) UpdateStatus(ctx context.Context, testType *example3iov1.TestType, opts v1.UpdateOptions) (*example3iov1.TestType, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(testtypesResource, "status", c.ns, testType), &example3iov1.TestType{})

	if obj == nil {
		return nil, err
	}
	return obj.(*example3iov1.TestType), err
}

// Delete takes name of the testType and deletes it. Returns an error if one occurs.
func (c *FakeTestTypes) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(testtypesResource, c.ns, name), &example3iov1.TestType{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeTestTypes) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(testtypesResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &example3iov1.TestTypeList{})
	return err
}

// Patch applies the patch and returns the patched testType.
func (c *FakeTestTypes) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *example3iov1.TestType, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(testtypesResource, c.ns, name, pt, data, subresources...), &example3iov1.TestType{})

	if obj == nil {
		return nil, err
	}
	return obj.(*example3iov1.TestType), err
}
