/*
Copyright 2023.

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

package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron"
	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "tutorial.kubebuilder.io/project/api/v1"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() }

// clock interface for teststing purpose
type Clock interface {
	Now() time.Time
}

//Additional privilages for controller to create jobs
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//

var (
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
)

// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.1/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the CronJob instance
	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "Unable to get the resource from NameSpace %s with name %s", req.Namespace, req.Name)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// List all jobs owned by this CronJob instance
	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "Unable to list the jobs for the CronJob %s in namespace %s", req.Name, req.Namespace)
		return ctrl.Result{}, err
	}

	// Catogarize jobs
	var activeJobs []*kbatch.Job
	var successfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var mostRecentTime *time.Time

	//function to get scheduled time for job
	getScheduledTimeForJob := func(job *kbatch.Job) (*time.Time, error) {
		timeRaw := job.Annotations[scheduledTimeAnnotation]
		if len(timeRaw) == 0 {
			return nil, nil
		}

		timeParsed, err := time.Parse(time.RFC3339, timeRaw)
		if err != nil {
			return nil, err
		}
		return &timeParsed, nil
	}

	// iterate over jobs and catogarize them
	for _, job := range childJobs.Items {
		var active bool = true
		for _, jobCondition := range job.Status.Conditions {
			if jobCondition.Status == corev1.ConditionTrue {
				if jobCondition.Type == kbatch.JobComplete {
					successfulJobs = append(successfulJobs, &job)
					active = false
					break
				} else if jobCondition.Type == kbatch.JobFailed {
					failedJobs = append(failedJobs, &job)
					active = false
					break
				}
			}
		}

		if active {
			activeJobs = append(activeJobs, &job)
		}

		// get scheduled time for job
		scheduledTime, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "Unable to parse the scheduled time for job %s in namespace %s", job.Name, job.Namespace)
			continue
		}

		// update most recent time
		if scheduledTime != nil && (mostRecentTime == nil || scheduledTime.After(*mostRecentTime)) {
			mostRecentTime = scheduledTime
		}
	}

	//update Cronjob status
	if mostRecentTime == nil {
		cronJob.Status.LastScheduleTime = nil
	} else {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	}

	cronJob.Status.Active = nil

	for _, job := range activeJobs {
		ref, err := ref.GetReference(r.Scheme, job)
		if err != nil {
			log.Error(err, "Unable to make reference for job %s in namespace %s", job.Name, job.Namespace)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *ref)
	}

	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	// update status subresouce
	if err := r.Status().Update(ctx, &cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	//delete old failed jobs
	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old failed job", "job", job)
			} else {
				log.V(0).Info("deleted old failed job", "job", job)
			}
		}
	}

	//delete old successful jobs
	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); (err) != nil {
				log.Error(err, "unable to delete old successful job", "job", job)
			} else {
				log.V(0).Info("deleted old successful job", "job", job)
			}
		}
	}

	// check if cronJob is suspended
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob is suspended, Set suspend to false to resume")
		return ctrl.Result{}, nil
	}

	// function to get nxt schedule
	getNextSchedule := func(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
		sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("Unparseable schedule %q: %v", cronJob.Spec.Schedule, err)
		}

		var earliestTime time.Time
		if cronJob.Status.LastScheduleTime != nil {
			earliestTime = cronJob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
		}
		if cronJob.Spec.StartingDeadlineSeconds != nil {

			schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))

			// check if we missed the deadline to start a job
			if schedulingDeadline.After(earliestTime) {
				earliestTime = schedulingDeadline
			}
		}

		// the next schedule is in future
		if earliestTime.After(now) {
			return time.Time{}, sched.Next(now), nil
		}

		starts := 0

		//check how many time the job has missed
		for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
			lastMissed = t

			starts++
			if starts > 100 {
				// missed schedule by many times so return error
				return time.Time{}, time.Time{}, fmt.Errorf("Too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew.")
			}
		}
		return lastMissed, sched.Next(now), nil
	}

	// get next schedule
	lastMissedTime, nextSchedule, err := getNextSchedule(&cronJob, r.Now())

	if err != nil {
		log.Error(err, "unable to get next schedule")
		return ctrl.Result{}, err
	}

	scheduledResult := ctrl.Result{RequeueAfter: nextSchedule.Sub(r.Now())}
	log = log.WithValues("now", r.Now(), "next run", nextSchedule)

	if lastMissedTime.IsZero() {
		log.V(1).Info("no missed start time")
		// reque
		return scheduledResult, nil
	}

	log = log.WithValues("current schedule", lastMissedTime)
	exceddedDeadline := false

	if cronJob.Spec.StartingDeadlineSeconds != nil {
		// check if excedded deadline
		exceddedDeadline = lastMissedTime.Add(time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds)).Before(r.Now())
	}

	if exceddedDeadline {
		log.V(1).Info("missed starting deadline , requing for next schedule")
		return scheduledResult, nil
	}

	// check for concurrency policy
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("concurrency policy forbids concurrent runs, skipping job creation")
		return scheduledResult, nil
	}

	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("concurrency policy is to replace concurrent runs, deleting job")
		for _, job := range activeJobs {
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old active job", "job", job)
			} else {
				log.V(0).Info("deleted old active job", "job", job)
			}
		}
	}

	constructJobForCronJob := func(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatch.Job, error) {

		// name for job
		name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

		job := &kbatch.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      make(map[string]string),
				Annotations: make(map[string]string),
				Name:        name,
				Namespace:   cronJob.Namespace,
			},
			Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
		}

		for k, v := range cronJob.Spec.JobTemplate.Annotations {
			job.Annotations[k] = v
		}

		job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
		for k, v := range cronJob.Spec.JobTemplate.Labels {
			job.Labels[k] = v
		}
		// set cronJob as owner of Job
		if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
			return nil, err
		}

		return job, nil
	}

	job, err := constructJobForCronJob(&cronJob, lastMissedTime)

	if err != nil {
		log.Error(err, "unable to create job from JobTemplate")
		return scheduledResult, err
	}

	// create job on cluster
	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create Job for CronJob", "job", job)
		return ctrl.Result{}, err
	}

	log.V(1).Info("created Job for CronJob run", "job", job)

	return scheduledResult, nil
}

var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr    = batchv1.GroupVersion.String()
)

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {

	if r.Clock == nil {
		r.Clock = realClock{}
	}

	// indexer to query jobs
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Complete(r)
}
