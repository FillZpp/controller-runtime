package manager

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var _ = Describe("runnables", func() {
	errCh := make(chan error)

	It("should be able to create a new runnables object", func() {
		Expect(newRunnables(errCh)).ToNot(BeNil())
	})

	It("should add caches to the appropriate group", func() {
		cache := &cacheProvider{cache: &informertest.FakeInformers{Error: fmt.Errorf("expected error")}}
		r := newRunnables(errCh)
		Expect(r.Add(cache, nil)).To(Succeed())
		var found *readyRunnable
		r.Caches.group.Range(func(key, value interface{}) bool {
			found = key.(*readyRunnable)
			// Only iterate once.
			return false
		})
		Expect(found).ToNot(BeNil())
		Expect(found.Runnable).To(BeIdenticalTo(cache))
	})

	It("should add webhooks to the appropriate group", func() {
		webhook := &webhook.Server{}
		r := newRunnables(errCh)
		Expect(r.Add(webhook, nil)).To(Succeed())
		var found *readyRunnable
		r.Webhooks.group.Range(func(key, value interface{}) bool {
			found = key.(*readyRunnable)
			// Only iterate once.
			return false
		})
		Expect(found).ToNot(BeNil())
		Expect(found.Runnable).To(BeIdenticalTo(webhook))
	})

	It("should add any runnable to the leader election group", func() {
		err := errors.New("runnable func")
		runnable := RunnableFunc(func(c context.Context) error {
			return err
		})

		r := newRunnables(errCh)
		Expect(r.Add(runnable, nil)).To(Succeed())
		var found *readyRunnable
		r.LeaderElection.group.Range(func(key, value interface{}) bool {
			found = key.(*readyRunnable)
			// Only iterate once.
			return false
		})
		Expect(found).ToNot(BeNil())

		// Functions are not comparable, we just make sure it's the same type and it returns what we expect
		Expect(found.Runnable).To(BeAssignableToTypeOf(runnable))
		Expect(found.Runnable.Start(context.Background())).To(MatchError(err))
	})
})

var _ = Describe("runnableGroup", func() {
	errCh := make(chan error)

	It("should be able to add new runnables before it starts", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rg := newRunnableGroup(errCh)
		Expect(rg.Add(RunnableFunc(func(c context.Context) error {
			<-ctx.Done()
			return nil
		}), nil)).To(Succeed())

		Expect(rg.Started()).To(BeFalse())
	})

	It("should be able to add new runnables before and after start", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rg := newRunnableGroup(errCh)
		Expect(rg.Add(RunnableFunc(func(c context.Context) error {
			<-ctx.Done()
			return nil
		}), nil)).To(Succeed())
		rg.Start()
		Expect(rg.Started()).To(BeTrue())
		Expect(rg.Add(RunnableFunc(func(c context.Context) error {
			<-ctx.Done()
			return nil
		}), nil)).To(Succeed())
		Expect(rg.WaitReady(ctx)).To(Succeed())
	})

	It("should be able to add new runnables before and after start concurrently", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rg := newRunnableGroup(errCh)

		go func() {
			<-time.After(50 * time.Millisecond)
			rg.Start()
		}()

		for i := 0; i < 20; i++ {
			go func(i int) {
				defer GinkgoRecover()

				<-time.After(time.Duration(i) * 10 * time.Millisecond)
				Expect(rg.Add(RunnableFunc(func(c context.Context) error {
					<-ctx.Done()
					return nil
				}), nil)).To(Succeed())
			}(i)
		}
		Expect(rg.WaitReady(ctx)).To(Succeed())
		Eventually(func() int {
			i := 0
			rg.group.Range(func(key, value interface{}) bool {
				i++
				return true
			})
			return i
		}).Should(BeNumerically("==", 20))
	})

	It("should be able to close the group and wait for all runnables to finish", func() {
		ctx, cancel := context.WithCancel(context.Background())

		exited := pointer.Int64(0)
		rg := newRunnableGroup(errCh)
		for i := 0; i < 10; i++ {
			Expect(rg.Add(RunnableFunc(func(c context.Context) error {
				defer atomic.AddInt64(exited, 1)
				<-ctx.Done()
				<-time.After(time.Duration(i) * 10 * time.Millisecond)
				return nil
			}), nil)).To(Succeed())
		}
		Expect(rg.StartAndWaitReady(ctx)).To(Succeed())

		// Cancel the context, asking the runnables to exit.
		cancel()
		rg.StopAndWait(context.Background())

		Expect(rg.Add(RunnableFunc(func(c context.Context) error {
			return nil
		}), nil)).ToNot(Succeed())

		Expect(atomic.LoadInt64(exited)).To(BeNumerically("==", 10))
	})

	It("should be able to wait for all runnables to be ready at different intervals", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		rg := newRunnableGroup(errCh)

		go func() {
			<-time.After(50 * time.Millisecond)
			rg.Start()
		}()

		for i := 0; i < 20; i++ {
			go func(i int) {
				defer GinkgoRecover()

				Expect(rg.Add(RunnableFunc(func(c context.Context) error {
					<-ctx.Done()
					return nil
				}), func(_ context.Context) bool {
					<-time.After(time.Duration(i) * 10 * time.Millisecond)
					return true
				})).To(Succeed())
			}(i)
		}
		Expect(rg.WaitReady(ctx)).To(Succeed())
		Eventually(func() int {
			i := 0
			rg.group.Range(func(key, value interface{}) bool {
				i++
				return true
			})
			return i
		}).Should(BeNumerically("==", 20))
	})

	It("should not turn ready if some readiness check fail", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		rg := newRunnableGroup(errCh)

		go func() {
			<-time.After(50 * time.Millisecond)
			rg.Start()
		}()

		for i := 0; i < 20; i++ {
			go func(i int) {
				defer GinkgoRecover()

				Expect(rg.Add(RunnableFunc(func(c context.Context) error {
					<-ctx.Done()
					return nil
				}), func(_ context.Context) bool {
					<-time.After(time.Duration(i) * 10 * time.Millisecond)
					return i%2 == 0 // Return false readiness all uneven indexes.
				})).To(Succeed())
			}(i)
		}
		Expect(rg.WaitReady(ctx)).ToNot(Succeed())
	})
})
