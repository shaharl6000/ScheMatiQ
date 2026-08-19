"""Process-wide singletons for stateful orchestration services.

``ScheMatiQRunner``, ``ReextractionService`` and ``ContinueDiscoveryService``
each hold in-process, per-session operation registries — the running asyncio
tasks and their stop flags (``running_sessions``/``stop_flags`` on the runner;
``active_operations``/``stop_flags``/``_extraction_tasks`` on reextraction;
``active_operations``/``stop_flags``/``_tasks`` on continue-discovery). Any code
that *starts* a run and any code that *stops* or *resumes* it must operate on the
same instance, or the stop/resume never sees the in-flight task and silently
no-ops.

Historically the HTTP routes (``routes/schematiq.py``, ``routes/schema.py``) and
the workspace chat (``services/chat/deps.py``) each constructed their own copies.
A run started from chat therefore could not be stopped by the route's Stop button
(and vice versa), and ``prepare_resume``'s instance-local ``is_running`` check
diverged from the shared ``concurrency_limiter``, forcing the fragile
"slot held without running task" branch. This module makes the three services
true singletons: every entry point imports the instances from here.

Import contract: this module MUST NOT be imported from ``app.services.__init__``.
The service classes below do ``from app.services import concurrency_limiter`` at
module load, so importing this module during package initialisation would hit a
partially-initialised ``app.services``. It is only safe to import once the
package has finished initialising — i.e. from route modules / ``chat.deps`` at
their own load time.
"""

from __future__ import annotations

from app.services import (
    data_collection_service,
    pubmed_enrichment_service,
    session_manager,
    uniprot_enrichment_service,
    websocket_manager,
)
from app.services.continue_discovery_service import ContinueDiscoveryService
from app.services.reextraction_service import ReextractionService
from app.services.schematiq_runner import ScheMatiQRunner

schematiq_runner = ScheMatiQRunner(
    websocket_manager=websocket_manager,
    session_manager=session_manager,
    data_collection_service=data_collection_service,
    pubmed_enrichment_service=pubmed_enrichment_service,
    uniprot_enrichment_service=uniprot_enrichment_service,
)

reextraction_service = ReextractionService(
    websocket_manager,
    session_manager,
    data_collection_service=data_collection_service,
    pubmed_enrichment_service=pubmed_enrichment_service,
    uniprot_enrichment_service=uniprot_enrichment_service,
)

continue_discovery_service = ContinueDiscoveryService(
    websocket_manager,
    session_manager,
    data_collection_service=data_collection_service,
    pubmed_enrichment_service=pubmed_enrichment_service,
    uniprot_enrichment_service=uniprot_enrichment_service,
)

__all__ = [
    "schematiq_runner",
    "reextraction_service",
    "continue_discovery_service",
]
