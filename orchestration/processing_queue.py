"""
Asynchronous document processing queue management.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import json
from models.document import Document
from core.document_manager import DocumentManager


logger = logging.getLogger(__name__)


class ProcessingStatus(Enum):
    """Processing status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProcessingTask:
    """Represents a document processing task."""
    task_id: str
    document: Document
    metadata: Dict
    status: ProcessingStatus = ProcessingStatus.PENDING
    created_at: datetime = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    priority: int = 0  # Higher numbers = higher priority
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
    
    def to_dict(self) -> Dict:
        """Convert task to dictionary."""
        return {
            'task_id': self.task_id,
            'document': self.document.to_dict(),
            'metadata': self.metadata,
            'status': self.status.value,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'result': self.result,
            'error': self.error,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'priority': self.priority
        }


class ProcessingQueue:
    """Asynchronous document processing queue."""
    
    def __init__(self, document_manager: DocumentManager, max_workers: int = 3):
        """
        Initialize processing queue.
        
        Args:
            document_manager: Document manager instance
            max_workers: Maximum number of concurrent workers
        """
        self.document_manager = document_manager
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
        
        # Queue management
        self.tasks: Dict[str, ProcessingTask] = {}
        self.pending_queue: List[str] = []  # Task IDs sorted by priority
        self.processing_tasks: Dict[str, asyncio.Task] = {}
        self.completed_tasks: List[str] = []
        self.failed_tasks: List[str] = []
        
        # Control flags
        self.is_running = False
        self.is_paused = False
        
        # Statistics
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'cancelled_tasks': 0,
            'processing_time_total': 0.0,
            'average_processing_time': 0.0
        }
        
        # Progress callback
        self.progress_callback: Optional[Callable] = None
    
    def add_task(
        self,
        document: Document,
        metadata: Dict,
        priority: int = 0
    ) -> str:
        """
        Add a document processing task to the queue.
        
        Args:
            document: Document to process
            metadata: Document metadata
            priority: Task priority (higher = more important)
            
        Returns:
            Task ID
        """
        task_id = f"{document.file_id}_{datetime.utcnow().timestamp()}"
        
        task = ProcessingTask(
            task_id=task_id,
            document=document,
            metadata=metadata,
            priority=priority
        )
        
        self.tasks[task_id] = task
        self.pending_queue.append(task_id)
        
        # Sort by priority (higher priority first)
        self.pending_queue.sort(key=lambda tid: self.tasks[tid].priority, reverse=True)
        
        self.stats['total_tasks'] += 1
        
        self.logger.info(f"Added task {task_id} for document {document.filename}")
        return task_id
    
    def add_batch_tasks(
        self,
        documents_with_metadata: List[Dict],
        priority: int = 0
    ) -> List[str]:
        """
        Add multiple tasks to the queue.
        
        Args:
            documents_with_metadata: List of {document, metadata} dicts
            priority: Task priority
            
        Returns:
            List of task IDs
        """
        task_ids = []
        
        for item in documents_with_metadata:
            task_id = self.add_task(
                item['document'],
                item['metadata'],
                priority
            )
            task_ids.append(task_id)
        
        return task_ids
    
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """
        Get task status.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status dictionary
        """
        if task_id not in self.tasks:
            return None
        
        task = self.tasks[task_id]
        return task.to_dict()
    
    def get_queue_status(self) -> Dict:
        """
        Get overall queue status.
        
        Returns:
            Queue status dictionary
        """
        return {
            'is_running': self.is_running,
            'is_paused': self.is_paused,
            'pending_tasks': len(self.pending_queue),
            'processing_tasks': len(self.processing_tasks),
            'completed_tasks': len(self.completed_tasks),
            'failed_tasks': len(self.failed_tasks),
            'total_tasks': len(self.tasks),
            'max_workers': self.max_workers,
            'statistics': self.stats
        }
    
    def get_pending_tasks(self) -> List[Dict]:
        """Get list of pending tasks."""
        return [self.tasks[task_id].to_dict() for task_id in self.pending_queue]
    
    def get_processing_tasks(self) -> List[Dict]:
        """Get list of currently processing tasks."""
        return [self.tasks[task_id].to_dict() for task_id in self.processing_tasks.keys()]
    
    def get_completed_tasks(self) -> List[Dict]:
        """Get list of completed tasks."""
        return [self.tasks[task_id].to_dict() for task_id in self.completed_tasks]
    
    def get_failed_tasks(self) -> List[Dict]:
        """Get list of failed tasks."""
        return [self.tasks[task_id].to_dict() for task_id in self.failed_tasks]
    
    async def process_single_task(self, task_id: str) -> Dict:
        """
        Process a single task.
        
        Args:
            task_id: Task ID to process
            
        Returns:
            Processing result
        """
        task = self.tasks[task_id]
        
        try:
            # Update task status
            task.status = ProcessingStatus.PROCESSING
            task.started_at = datetime.utcnow()
            
            # Notify progress callback
            if self.progress_callback:
                self.progress_callback(task.to_dict())
            
            self.logger.info(f"Processing task {task_id}: {task.document.filename}")
            
            # Process document
            result = self.document_manager.process_document(
                task.document,
                task.metadata
            )
            
            # Update task with result
            task.completed_at = datetime.utcnow()
            task.result = result
            
            if result.get('success', False):
                task.status = ProcessingStatus.COMPLETED
                self.completed_tasks.append(task_id)
                self.stats['completed_tasks'] += 1
            else:
                task.status = ProcessingStatus.FAILED
                task.error = result.get('error', 'Unknown error')
                self.failed_tasks.append(task_id)
                self.stats['failed_tasks'] += 1
            
            # Update processing time statistics
            if task.started_at and task.completed_at:
                processing_time = (task.completed_at - task.started_at).total_seconds()
                self.stats['processing_time_total'] += processing_time
                completed_count = self.stats['completed_tasks'] + self.stats['failed_tasks']
                if completed_count > 0:
                    self.stats['average_processing_time'] = self.stats['processing_time_total'] / completed_count
            
            # Notify progress callback
            if self.progress_callback:
                self.progress_callback(task.to_dict())
            
            self.logger.info(f"Task {task_id} completed with status: {task.status.value}")
            return task.to_dict()
            
        except Exception as e:
            # Handle task failure
            task.status = ProcessingStatus.FAILED
            task.error = str(e)
            task.completed_at = datetime.utcnow()
            
            self.failed_tasks.append(task_id)
            self.stats['failed_tasks'] += 1
            
            self.logger.error(f"Task {task_id} failed: {str(e)}")
            
            # Notify progress callback
            if self.progress_callback:
                self.progress_callback(task.to_dict())
            
            return task.to_dict()
    
    async def worker(self, worker_id: int) -> None:
        """
        Worker coroutine that processes tasks.
        
        Args:
            worker_id: Worker identifier
        """
        self.logger.info(f"Worker {worker_id} started")
        
        while self.is_running:
            try:
                # Check if paused
                if self.is_paused:
                    await asyncio.sleep(1)
                    continue
                
                # Get next task
                if not self.pending_queue:
                    await asyncio.sleep(0.1)
                    continue
                
                task_id = self.pending_queue.pop(0)
                
                # Add to processing tasks
                self.processing_tasks[task_id] = asyncio.current_task()
                
                # Process the task
                await self.process_single_task(task_id)
                
                # Remove from processing tasks
                if task_id in self.processing_tasks:
                    del self.processing_tasks[task_id]
                
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {str(e)}")
                await asyncio.sleep(1)
        
        self.logger.info(f"Worker {worker_id} stopped")
    
    async def start(self) -> None:
        """Start the processing queue."""
        if self.is_running:
            self.logger.warning("Processing queue already running")
            return
        
        self.is_running = True
        self.is_paused = False
        
        # Start worker tasks
        worker_tasks = []
        for i in range(self.max_workers):
            task = asyncio.create_task(self.worker(i))
            worker_tasks.append(task)
        
        self.logger.info(f"Processing queue started with {self.max_workers} workers")
        
        # Wait for all workers to complete
        await asyncio.gather(*worker_tasks)
    
    def pause(self) -> None:
        """Pause the processing queue."""
        self.is_paused = True
        self.logger.info("Processing queue paused")
    
    def resume(self) -> None:
        """Resume the processing queue."""
        self.is_paused = False
        self.logger.info("Processing queue resumed")
    
    def stop(self) -> None:
        """Stop the processing queue."""
        self.is_running = False
        self.is_paused = False
        
        # Cancel all processing tasks
        for task in self.processing_tasks.values():
            task.cancel()
        
        self.logger.info("Processing queue stopped")
    
    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a specific task.
        
        Args:
            task_id: Task ID to cancel
            
        Returns:
            True if cancelled successfully
        """
        if task_id not in self.tasks:
            return False
        
        task = self.tasks[task_id]
        
        # Remove from pending queue
        if task_id in self.pending_queue:
            self.pending_queue.remove(task_id)
            task.status = ProcessingStatus.CANCELLED
            self.stats['cancelled_tasks'] += 1
            self.logger.info(f"Task {task_id} cancelled")
            return True
        
        # Cancel processing task
        if task_id in self.processing_tasks:
            asyncio_task = self.processing_tasks[task_id]
            asyncio_task.cancel()
            task.status = ProcessingStatus.CANCELLED
            self.stats['cancelled_tasks'] += 1
            self.logger.info(f"Processing task {task_id} cancelled")
            return True
        
        return False
    
    def clear_completed(self) -> int:
        """
        Clear completed tasks from memory.
        
        Returns:
            Number of tasks cleared
        """
        cleared_count = 0
        
        # Clear completed tasks
        for task_id in self.completed_tasks[:]:
            if task_id in self.tasks:
                del self.tasks[task_id]
                cleared_count += 1
        
        self.completed_tasks.clear()
        
        # Clear failed tasks
        for task_id in self.failed_tasks[:]:
            if task_id in self.tasks:
                del self.tasks[task_id]
                cleared_count += 1
        
        self.failed_tasks.clear()
        
        self.logger.info(f"Cleared {cleared_count} completed/failed tasks")
        return cleared_count
    
    def set_progress_callback(self, callback: Callable) -> None:
        """
        Set progress callback function.
        
        Args:
            callback: Function to call on progress updates
        """
        self.progress_callback = callback
        self.logger.info("Progress callback set")
    
    def get_task_history(self, limit: int = 100) -> List[Dict]:
        """
        Get task history.
        
        Args:
            limit: Maximum number of tasks to return
            
        Returns:
            List of task dictionaries sorted by creation time
        """
        all_tasks = list(self.tasks.values())
        all_tasks.sort(key=lambda t: t.created_at, reverse=True)
        
        return [task.to_dict() for task in all_tasks[:limit]]