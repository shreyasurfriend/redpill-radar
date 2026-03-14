import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models import Content
from app.services.analyzer import analyze_content

logger = logging.getLogger(__name__)


async def analyze_single(content_id: str):
    """Background task: run Groq analysis on a newly ingested content item."""
    async with async_session() as db:
        result = await db.execute(select(Content).where(Content.id == content_id))
        record = result.scalar_one_or_none()
        if not record:
            logger.error("Background analyze: content %s not found", content_id)
            return

        try:
            analysis, raw_response = await analyze_content(record.content_text, db)

            record.age_category = analysis.age_category
            record.content_type = analysis.content_type
            record.harmful_subcategories = analysis.harmful_subcategories
            record.labels = analysis.model_dump()
            record.raw_analysis = raw_response
            record.analysis_status = "completed"

            history = list(record.processing_history or [])
            history.append(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "action": "analyzed",
                    "old_value": None,
                    "new_value": None,
                    "comment": "Groq analysis completed in background",
                }
            )
            record.processing_history = history
            await db.commit()
            logger.info("Background analysis completed for content %s", content_id)
        except Exception:
            logger.exception("Background analysis failed for content %s", content_id)
            record.analysis_status = "failed"
            await db.commit()


async def reprocess_single(content_id: str, db: AsyncSession) -> Content:
    """Re-analyze a single content item with Groq and update its record."""
    result = await db.execute(select(Content).where(Content.id == content_id))
    record = result.scalar_one_or_none()
    if not record:
        raise ValueError(f"Content {content_id} not found")

    analysis, raw_response = await analyze_content(record.content_text, db)

    record.age_category = analysis.age_category
    record.content_type = analysis.content_type
    record.harmful_subcategories = analysis.harmful_subcategories
    record.labels = analysis.model_dump()
    record.raw_analysis = raw_response

    history = list(record.processing_history or [])
    history.append(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "reprocessed",
            "old_value": None,
            "new_value": None,
            "comment": "Content re-analyzed via Groq",
        }
    )
    record.processing_history = history

    await db.commit()
    await db.refresh(record)
    return record


async def reprocess_all_unprocessed():
    """Background task: re-analyze all unprocessed content items."""
    async with async_session() as db:
        result = await db.execute(
            select(Content).where(Content.is_processed.is_(False))
        )
        records = result.scalars().all()
        logger.info("Reprocessing %d unprocessed content items", len(records))

        for record in records:
            try:
                analysis, raw_response = await analyze_content(
                    record.content_text, db
                )
                record.age_category = analysis.age_category
                record.content_type = analysis.content_type
                record.harmful_subcategories = analysis.harmful_subcategories
                record.labels = analysis.model_dump()
                record.raw_analysis = raw_response
                record.analysis_status = "completed"

                history = list(record.processing_history or [])
                history.append(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "action": "bulk_reprocessed",
                        "old_value": None,
                        "new_value": None,
                        "comment": "Bulk re-analysis via background task",
                    }
                )
                record.processing_history = history
                await db.commit()
            except Exception:
                logger.exception("Failed to reprocess content %s", record.id)
                await db.rollback()

        logger.info("Bulk reprocessing complete")
