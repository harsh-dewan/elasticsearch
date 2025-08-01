/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.OperationModeUpdateTask;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.io.Closeable;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentSLMMode;

/**
 * {@code SnapshotLifecycleService} manages snapshot policy scheduling and triggering of the
 * {@link SnapshotLifecycleTask}. It reacts to new policies in the cluster state by scheduling a
 * task according to the policy's schedule.
 */
public class SnapshotLifecycleService implements Closeable, ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SnapshotLifecycleService.class);
    private static final String JOB_PATTERN_SUFFIX = "-\\d+$";

    private final ClusterService clusterService;
    private final Function<ProjectId, SnapshotLifecycleTask> taskProvider;
    private final Settings settings;
    private final Clock clock;
    private volatile boolean isMaster = false;
    private final ConcurrentMap<ProjectId, SnapshotLifecycleProjectState> projects = ConcurrentCollections.newConcurrentMap();

    public SnapshotLifecycleService(
        Settings settings,
        Function<ProjectId, SnapshotLifecycleTask> taskProvider,
        ClusterService clusterService,
        Clock clock
    ) {
        this.settings = settings;
        this.taskProvider = taskProvider;
        this.clusterService = clusterService;
        this.clock = clock;
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
    }

    @Override
    @FixForMultiProject(description = "Project deletion should cancel the scheduled SLM tasks")
    public void clusterChanged(final ClusterChangedEvent event) {
        // Instead of using a LocalNodeMasterListener to track master changes, this service will
        // track them here to avoid conditions where master listener events run after other
        // listeners that depend on what happened in the master listener
        final boolean prevIsMaster = this.isMaster;
        final boolean masterChanged = prevIsMaster != event.localNodeMaster();
        this.isMaster = event.localNodeMaster();

        for (ProjectMetadata metadata : event.state().metadata().projects().values()) {
            SnapshotLifecycleProjectState project = getOrCreateProjectState(metadata.id());
            if (masterChanged) {
                if (this.isMaster) {
                    project.scheduler.register(project.snapshotTask);
                } else {
                    project.scheduler.unregister(project.snapshotTask);
                    cancelSnapshotJobs(project);
                }
            }

            if (this.isMaster) {
                if (slmStoppedOrStopping(metadata)) {
                    if (project.scheduler.scheduledJobIds().isEmpty() == false) {
                        cancelSnapshotJobs(project);
                    }
                    if (slmStopping(metadata)) {
                        submitUnbatchedTask("slm_operation_mode_update[stopped]", OperationModeUpdateTask.slmMode(OperationMode.STOPPED));
                    }
                    continue;
                }

                scheduleSnapshotJobs(project, metadata);
                cleanupDeletedPolicies(project, metadata);
            }
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    // Only used for testing
    SchedulerEngine getScheduler(ProjectId projectId) {
        SnapshotLifecycleProjectState project = projects.get(projectId);
        return project == null ? null : project.scheduler;
    }

    /**
     * Returns true if SLM is in the stopping or stopped state
     */
    @Deprecated(forRemoval = true)
    static boolean slmStoppedOrStopping(ClusterState state) {
        OperationMode mode = currentSLMMode(state);
        return OperationMode.STOPPING == mode || OperationMode.STOPPED == mode;
    }

    /**
     * Returns true if SLM is in the stopping or stopped state
     */
    static boolean slmStoppedOrStopping(ProjectMetadata metadata) {
        OperationMode mode = currentSLMMode(metadata);
        return OperationMode.STOPPING == mode || OperationMode.STOPPED == mode;
    }

    /**
     * Returns true if SLM is in the stopping state
     */
    static boolean slmStopping(ProjectMetadata metadata) {
        OperationMode mode = currentSLMMode(metadata);
        return OperationMode.STOPPING == mode;
    }

    /**
     * Schedule all non-scheduled snapshot jobs contained in the cluster state
     */
    private void scheduleSnapshotJobs(final SnapshotLifecycleProjectState project, final ProjectMetadata metadata) {
        SnapshotLifecycleMetadata snapMeta = metadata.custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta != null) {
            snapMeta.getSnapshotConfigurations().values().forEach((config) -> maybeScheduleSnapshot(project, config));
        }
    }

    private void cleanupDeletedPolicies(final SnapshotLifecycleProjectState project, final ProjectMetadata metadata) {
        SnapshotLifecycleMetadata snapMeta = metadata.custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta != null) {
            // Retrieve all the expected policy job ids from the policies in the metadata
            final Set<String> policyJobIds = snapMeta.getSnapshotConfigurations()
                .values()
                .stream()
                .map(SnapshotLifecycleService::getJobId)
                .collect(Collectors.toSet());

            // Cancel all jobs that are *NOT* in the scheduled tasks map
            project.scheduledTasks.keySet()
                .stream()
                .filter(jobId -> policyJobIds.contains(jobId) == false)
                .forEach(jobId -> cancelScheduledSnapshot(project, jobId));
        }
    }

    /**
     * Schedule the {@link SnapshotLifecyclePolicy} job if it does not already exist. First checks
     * to see if any previous versions of the policy were scheduled, and if so, cancels those. If
     * the same version of a policy has already been scheduled it does not overwrite the job.
     */
    void maybeScheduleSnapshot(final SnapshotLifecycleProjectState project, final SnapshotLifecyclePolicyMetadata snapshotLifecyclePolicy) {
        if (project.running.get() == false) {
            return;
        }

        final String jobId = getJobId(snapshotLifecyclePolicy);
        final Pattern existingJobPattern = Pattern.compile(snapshotLifecyclePolicy.getPolicy().getId() + JOB_PATTERN_SUFFIX);

        // Find and cancel any existing jobs for this policy
        final boolean existingJobsFoundAndCancelled = project.scheduledTasks.keySet()
            .stream()
            // Find all jobs matching the `jobid-\d+` pattern
            .filter(jId -> existingJobPattern.matcher(jId).matches())
            // Filter out a job that has not been changed (matches the id exactly meaning the version is the same)
            .filter(jId -> jId.equals(jobId) == false)
            .map(existingJobId -> {
                // Cancel existing job so the new one can be scheduled
                logger.debug("removing existing snapshot lifecycle job [{}] as it has been updated", existingJobId);
                project.scheduledTasks.remove(existingJobId);
                boolean existed = project.scheduler.remove(existingJobId);
                assert existed : "expected job for " + existingJobId + " to exist in scheduler";
                return existed;
            })
            .reduce(false, (a, b) -> a || b);

        // Now atomically schedule the new job and add it to the scheduled tasks map. If the jobId
        // is identical to an existing job (meaning the version has not changed) then this does
        // not reschedule it.
        project.scheduledTasks.computeIfAbsent(jobId, id -> {
            if (existingJobsFoundAndCancelled) {
                logger.info("rescheduling updated snapshot lifecycle job [{}]", jobId);
            } else {
                logger.info("scheduling snapshot lifecycle job [{}]", jobId);
            }

            final SchedulerEngine.Job job = snapshotLifecyclePolicy.buildSchedulerJob(jobId);
            project.scheduler.add(job);
            return job;
        });
    }

    /**
     * Generate the job id for a given policy metadata. The job id is {@code <policyid>-<version>}
     */
    public static String getJobId(SnapshotLifecyclePolicyMetadata policyMeta) {
        return policyMeta.getPolicy().getId() + "-" + policyMeta.getVersion();
    }

    /**
     * Cancel all scheduled snapshot jobs
     */
    private void cancelSnapshotJobs(SnapshotLifecycleProjectState project) {
        logger.trace("cancelling all snapshot lifecycle jobs");
        project.scheduler.scheduledJobIds().forEach(project.scheduler::remove);
        project.scheduledTasks.clear();
    }

    /**
     * Cancel the given policy job id (from {@link #getJobId(SnapshotLifecyclePolicyMetadata)}
     */
    private void cancelScheduledSnapshot(final SnapshotLifecycleProjectState project, final String lifecycleJobId) {
        logger.debug("cancelling project [{}] snapshot lifecycle job [{}] as it no longer exists", project.projectId, lifecycleJobId);
        project.scheduledTasks.remove(lifecycleJobId);
        project.scheduler.remove(lifecycleJobId);
    }

    private SnapshotLifecycleProjectState getOrCreateProjectState(ProjectId projectId) {
        // initialize the project state if it does not exist (i.e. new project)
        return projects.computeIfAbsent(
            projectId,
            id -> new SnapshotLifecycleProjectState(projectId, new SchedulerEngine(settings, clock), taskProvider.apply(projectId))
        );
    }

    /**
     * Validates that the {@code repository} exists as a registered snapshot repository
     * @throws IllegalArgumentException if the repository does not exist
     */
    @Deprecated(forRemoval = true)
    public static void validateRepositoryExists(final String repository, final ClusterState state) {
        if (RepositoriesMetadata.get(state).repository(repository) == null) {
            throw new IllegalArgumentException("no such repository [" + repository + "]");
        }
    }

    /**
     * Validates that the interval between snapshots is not smaller than the minimum interval
     * (see {@link LifecycleSettings#SLM_MINIMUM_INTERVAL_SETTING})
     * @throws IllegalArgumentException if the interval is less than the minimum
     */
    public static void validateMinimumInterval(final SnapshotLifecyclePolicy lifecycle, final ClusterState state) {
        TimeValue minimum = LifecycleSettings.SLM_MINIMUM_INTERVAL_SETTING.get(state.metadata().settings());
        TimeValue next = lifecycle.calculateNextInterval(Clock.systemUTC());
        if (next.duration() > 0 && minimum.duration() > 0 && next.millis() < minimum.millis()) {
            throw new IllegalArgumentException(
                "invalid schedule ["
                    + lifecycle.getSchedule()
                    + "]: "
                    + "schedule would be too frequent, executing more than every ["
                    + minimum.getStringRep()
                    + "]"
            );
        }
    }

    @Override
    public void close() {
        projects.values().forEach(project -> {
            if (project.running.compareAndSet(true, false)) {
                project.scheduler.stop();
            }
        });
    }

    /**
     * Internal wrapper class to hold the state of a project's snapshot lifecycle
     */
    static class SnapshotLifecycleProjectState {
        final ProjectId projectId;
        final SchedulerEngine scheduler;
        final SnapshotLifecycleTask snapshotTask;
        final Map<String, SchedulerEngine.Job> scheduledTasks = ConcurrentCollections.newConcurrentMap();
        final AtomicBoolean running = new AtomicBoolean(true);

        SnapshotLifecycleProjectState(ProjectId projectId, SchedulerEngine scheduler, SnapshotLifecycleTask snapshotTask) {
            this.projectId = projectId;
            this.scheduler = scheduler;
            this.snapshotTask = snapshotTask;
        }
    }
}
