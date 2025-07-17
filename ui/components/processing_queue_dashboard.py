"""
Processing queue dashboard component for real-time progress tracking.
"""

import streamlit as st
import time
from typing import Dict, List, Optional, Any
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd
from orchestration.processing_queue import ProcessingQueue, ProcessingStatus


class ProcessingQueueDashboard:
    """Dashboard component for monitoring processing queue."""
    
    @staticmethod
    def render_queue_overview(queue: ProcessingQueue) -> None:
        """
        Render queue overview with key metrics.
        
        Args:
            queue: Processing queue instance
        """
        st.subheader("🎯 Processing Queue Overview")
        
        queue_status = queue.get_queue_status()
        
        # Main metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Pending Tasks",
                queue_status['pending_tasks'],
                help="Tasks waiting to be processed"
            )
        
        with col2:
            st.metric(
                "Processing",
                queue_status['processing_tasks'],
                help="Tasks currently being processed"
            )
        
        with col3:
            st.metric(
                "Completed",
                queue_status['completed_tasks'],
                help="Successfully completed tasks"
            )
        
        with col4:
            st.metric(
                "Failed",
                queue_status['failed_tasks'],
                help="Tasks that failed processing"
            )
        
        # Progress bar
        if queue_status['total_tasks'] > 0:
            progress = (queue_status['completed_tasks'] + queue_status['failed_tasks']) / queue_status['total_tasks']
            st.progress(progress, text=f"Overall Progress: {progress:.1%}")
        
        # Queue status indicators
        col1, col2 = st.columns(2)
        
        with col1:
            if queue_status['is_running']:
                if queue_status['is_paused']:
                    st.warning("⏸️ Queue is PAUSED")
                else:
                    st.success("▶️ Queue is RUNNING")
            else:
                st.error("⏹️ Queue is STOPPED")
        
        with col2:
            st.info(f"👥 Workers: {queue_status['max_workers']}")
    
    @staticmethod
    def render_queue_statistics(queue: ProcessingQueue) -> None:
        """
        Render detailed queue statistics.
        
        Args:
            queue: Processing queue instance
        """
        st.subheader("📊 Processing Statistics")
        
        stats = queue.get_queue_status()['statistics']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "Total Tasks",
                stats['total_tasks'],
                help="Total number of tasks added to queue"
            )
            st.metric(
                "Success Rate",
                f"{(stats['completed_tasks'] / max(stats['total_tasks'], 1) * 100):.1f}%",
                help="Percentage of successfully completed tasks"
            )
        
        with col2:
            avg_time = stats.get('average_processing_time', 0)
            if avg_time > 0:
                if avg_time < 60:
                    time_str = f"{avg_time:.1f}s"
                else:
                    time_str = f"{avg_time/60:.1f}m"
            else:
                time_str = "N/A"
            
            st.metric(
                "Avg Processing Time",
                time_str,
                help="Average time to process a document"
            )
            
            st.metric(
                "Total Processing Time",
                f"{stats.get('processing_time_total', 0):.1f}s",
                help="Total time spent processing documents"
            )
    
    @staticmethod
    def render_task_list(
        tasks: List[Dict],
        title: str,
        status_color: str = "blue",
        show_details: bool = True
    ) -> None:
        """
        Render a list of tasks.
        
        Args:
            tasks: List of task dictionaries
            title: Section title
            status_color: Color for status indicators
            show_details: Whether to show detailed task information
        """
        if not tasks:
            st.info(f"No {title.lower()}")
            return
        
        st.subheader(f"📋 {title} ({len(tasks)})")
        
        for task in tasks:
            with st.expander(
                f"📄 {task['document']['filename']} - {task['status'].upper()}",
                expanded=False
            ):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Task ID:** {task['task_id']}")
                    st.write(f"**Document Type:** {task['metadata'].get('document_type', 'Unknown')}")
                    st.write(f"**File Size:** {task['document']['file_size']:,} bytes")
                    
                    # Machine names
                    machines = task['metadata'].get('machine_names', [])
                    if machines:
                        st.write(f"**Machines:** {', '.join(machines)}")
                
                with col2:
                    st.write(f"**Status:** {task['status'].upper()}")
                    st.write(f"**Priority:** {task['priority']}")
                    st.write(f"**Created:** {task['created_at']}")
                    
                    if task['started_at']:
                        st.write(f"**Started:** {task['started_at']}")
                    if task['completed_at']:
                        st.write(f"**Completed:** {task['completed_at']}")
                
                # Show processing details if available
                if show_details and task.get('result'):
                    result = task['result']
                    if result.get('success'):
                        st.success("✅ Processing successful")
                        if result.get('processor_used'):
                            st.write(f"**Processor:** {result['processor_used']}")
                    else:
                        st.error("❌ Processing failed")
                        if result.get('error'):
                            st.error(f"**Error:** {result['error']}")
                
                # Show error details
                if task.get('error'):
                    st.error(f"**Error:** {task['error']}")
                
                # Show retry information
                if task.get('retry_count', 0) > 0:
                    st.warning(f"**Retries:** {task['retry_count']}/{task['max_retries']}")
    
    @staticmethod
    def render_processing_controls(queue: ProcessingQueue) -> Dict[str, bool]:
        """
        Render processing control buttons.
        
        Args:
            queue: Processing queue instance
            
        Returns:
            Dictionary of control actions triggered
        """
        st.subheader("🎛️ Queue Controls")
        
        queue_status = queue.get_queue_status()
        
        col1, col2, col3, col4 = st.columns(4)
        
        controls = {}
        
        with col1:
            if queue_status['is_running']:
                if queue_status['is_paused']:
                    controls['resume'] = st.button("▶️ Resume", help="Resume processing")
                else:
                    controls['pause'] = st.button("⏸️ Pause", help="Pause processing")
            else:
                controls['start'] = st.button("▶️ Start", help="Start processing queue")
        
        with col2:
            if queue_status['is_running']:
                controls['stop'] = st.button("⏹️ Stop", help="Stop processing queue")
        
        with col3:
            if queue_status['completed_tasks'] > 0 or queue_status['failed_tasks'] > 0:
                controls['clear_completed'] = st.button(
                    "🗑️ Clear Completed",
                    help="Clear completed and failed tasks from memory"
                )
        
        with col4:
            controls['refresh'] = st.button("🔄 Refresh", help="Refresh queue status")
        
        return controls
    
    @staticmethod
    def render_task_progress_chart(queue: ProcessingQueue) -> None:
        """
        Render task progress chart.
        
        Args:
            queue: Processing queue instance
        """
        st.subheader("📈 Task Progress")
        
        queue_status = queue.get_queue_status()
        
        # Create pie chart for task status distribution
        labels = ['Pending', 'Processing', 'Completed', 'Failed']
        values = [
            queue_status['pending_tasks'],
            queue_status['processing_tasks'],
            queue_status['completed_tasks'],
            queue_status['failed_tasks']
        ]
        
        # Only show chart if there are tasks
        if sum(values) > 0:
            fig = px.pie(
                values=values,
                names=labels,
                title="Task Status Distribution",
                color_discrete_map={
                    'Pending': '#FFA500',
                    'Processing': '#1E90FF',
                    'Completed': '#32CD32',
                    'Failed': '#DC143C'
                }
            )
            
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No tasks to display")
    
    @staticmethod
    def render_processing_timeline(queue: ProcessingQueue) -> None:
        """
        Render processing timeline.
        
        Args:
            queue: Processing queue instance
        """
        st.subheader("⏱️ Processing Timeline")
        
        # Get recent task history
        history = queue.get_task_history(limit=20)
        
        if not history:
            st.info("No task history available")
            return
        
        # Create timeline data
        timeline_data = []
        for task in history:
            if task['started_at'] and task['completed_at']:
                start_time = datetime.fromisoformat(task['started_at'])
                end_time = datetime.fromisoformat(task['completed_at'])
                duration = (end_time - start_time).total_seconds()
                
                timeline_data.append({
                    'Task': task['document']['filename'][:30] + "...",
                    'Start': start_time,
                    'End': end_time,
                    'Duration': duration,
                    'Status': task['status'],
                    'Document Type': task['metadata'].get('document_type', 'Unknown')
                })
        
        if timeline_data:
            df = pd.DataFrame(timeline_data)
            
            # Create Gantt chart
            fig = px.timeline(
                df,
                x_start='Start',
                x_end='End',
                y='Task',
                color='Status',
                title="Recent Task Processing Timeline",
                color_discrete_map={
                    'completed': '#32CD32',
                    'failed': '#DC143C',
                    'cancelled': '#FFA500'
                }
            )
            
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Document",
                height=max(400, len(timeline_data) * 30)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No completed tasks to display in timeline")
    
    @staticmethod
    def render_real_time_monitor(queue: ProcessingQueue) -> None:
        """
        Render real-time monitoring dashboard.
        
        Args:
            queue: Processing queue instance
        """
        st.subheader("🔴 Real-Time Monitor")
        
        # Create containers for real-time updates
        status_container = st.container()
        tasks_container = st.container()
        
        # Auto-refresh every 5 seconds
        if st.button("🔄 Enable Auto-Refresh"):
            st.session_state['auto_refresh'] = True
        
        if st.session_state.get('auto_refresh', False):
            if st.button("⏸️ Disable Auto-Refresh"):
                st.session_state['auto_refresh'] = False
            
            # Auto-refresh placeholder
            time.sleep(1)
            st.rerun()
        
        with status_container:
            # Current processing status
            processing_tasks = queue.get_processing_tasks()
            
            if processing_tasks:
                st.write("**Currently Processing:**")
                for task in processing_tasks:
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.write(f"📄 {task['document']['filename']}")
                    with col2:
                        if task['started_at']:
                            start_time = datetime.fromisoformat(task['started_at'])
                            elapsed = (datetime.utcnow() - start_time).total_seconds()
                            st.write(f"⏱️ {elapsed:.0f}s")
            else:
                st.info("No tasks currently processing")
        
        with tasks_container:
            # Next up in queue
            pending_tasks = queue.get_pending_tasks()
            
            if pending_tasks:
                st.write("**Next in Queue:**")
                for task in pending_tasks[:5]:  # Show first 5
                    st.write(f"📄 {task['document']['filename']} (Priority: {task['priority']})")
                
                if len(pending_tasks) > 5:
                    st.write(f"... and {len(pending_tasks) - 5} more")
            else:
                st.info("No pending tasks")
    
    @staticmethod
    def render_full_dashboard(queue: ProcessingQueue) -> None:
        """
        Render complete processing queue dashboard.
        
        Args:
            queue: Processing queue instance
        """
        st.title("📊 Document Processing Queue Dashboard")
        
        # Overview section
        ProcessingQueueDashboard.render_queue_overview(queue)
        
        st.divider()
        
        # Statistics section
        ProcessingQueueDashboard.render_queue_statistics(queue)
        
        st.divider()
        
        # Controls section
        controls = ProcessingQueueDashboard.render_processing_controls(queue)
        
        # Handle control actions
        if controls.get('start'):
            st.success("Queue start requested")
        if controls.get('pause'):
            queue.pause()
            st.success("Queue paused")
        if controls.get('resume'):
            queue.resume()
            st.success("Queue resumed")
        if controls.get('stop'):
            queue.stop()
            st.success("Queue stopped")
        if controls.get('clear_completed'):
            cleared = queue.clear_completed()
            st.success(f"Cleared {cleared} completed tasks")
        if controls.get('refresh'):
            st.rerun()
        
        st.divider()
        
        # Task lists in tabs
        tab1, tab2, tab3, tab4 = st.tabs(["Pending", "Processing", "Completed", "Failed"])
        
        with tab1:
            ProcessingQueueDashboard.render_task_list(
                queue.get_pending_tasks(),
                "Pending Tasks",
                "orange"
            )
        
        with tab2:
            ProcessingQueueDashboard.render_task_list(
                queue.get_processing_tasks(),
                "Processing Tasks",
                "blue"
            )
        
        with tab3:
            ProcessingQueueDashboard.render_task_list(
                queue.get_completed_tasks(),
                "Completed Tasks",
                "green"
            )
        
        with tab4:
            ProcessingQueueDashboard.render_task_list(
                queue.get_failed_tasks(),
                "Failed Tasks",
                "red"
            )
        
        st.divider()
        
        # Charts section
        col1, col2 = st.columns(2)
        
        with col1:
            ProcessingQueueDashboard.render_task_progress_chart(queue)
        
        with col2:
            ProcessingQueueDashboard.render_processing_timeline(queue)