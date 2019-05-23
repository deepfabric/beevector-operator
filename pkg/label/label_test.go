// Copyright 2018 infinivision, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package label

import (
	"testing"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/labels"
)

func TestLabelNew(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	g.Expect(l[NameLabelKey]).To(Equal("hyena-cluster"))
	g.Expect(l[ManagedByLabelKey]).To(Equal("hyena-operator"))
}

func TestLabelInstance(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Instance("demo")
	g.Expect(l[InstanceLabelKey]).To(Equal("demo"))
}

func TestLabelNamespace(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Namespace("ns-1")
	g.Expect(l[NamespaceLabelKey]).To(Equal("ns-1"))
}

func TestLabelComponent(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Component("store")
	g.Expect(l.ComponentType()).To(Equal("store"))
}

func TestLabelProphet(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Prophet()
	g.Expect(l.IsProphet()).To(BeTrue())
}

func TestLabelStore(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Store()
	g.Expect(l.IsStore()).To(BeTrue())
}

func TestLabelSelector(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Prophet()
	l.Instance("demo")
	l.Namespace("ns-1")
	s, err := l.Selector()
	st := labels.Set(map[string]string{
		NameLabelKey:      "hyena-cluster",
		ManagedByLabelKey: "hyena-operator",
		ComponentLabelKey: "prophet",
		InstanceLabelKey:  "demo",
		NamespaceLabelKey: "ns-1",
	})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(s.Matches(st)).To(BeTrue())
}

func TestLabelLabelSelector(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Prophet()
	l.Instance("demo")
	l.Namespace("ns-1")
	ls := l.LabelSelector()
	m := map[string]string{
		NameLabelKey:      "hyena-cluster",
		ManagedByLabelKey: "hyena-operator",
		ComponentLabelKey: "prophet",
		InstanceLabelKey:  "demo",
		NamespaceLabelKey: "ns-1",
	}
	g.Expect(ls.MatchLabels).To(Equal(m))
}

func TestLabelLabels(t *testing.T) {
	g := NewGomegaWithT(t)

	l := New()
	l.Prophet()
	l.Instance("demo")
	l.Namespace("ns-1")
	ls := l.Labels()
	m := map[string]string{
		NameLabelKey:      "hyena-cluster",
		ManagedByLabelKey: "hyena-operator",
		ComponentLabelKey: "prophet",
		InstanceLabelKey:  "demo",
		NamespaceLabelKey: "ns-1",
	}
	g.Expect(ls).To(Equal(m))
}
