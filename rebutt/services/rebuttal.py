"""Core rebuttal generation service."""

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

from groq import AsyncGroq
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings

logger = logging.getLogger(__name__)

_groq_client: Optional[AsyncGroq] = None


def _get_groq_client() -> AsyncGroq:
    """Get or create Groq client."""
    global _groq_client
    if _groq_client is None:
        _groq_client = AsyncGroq(api_key=settings.groq_api_key)
    return _groq_client


class RebuttalService:
    """Service for generating rebuttals to harmful content."""
    
    def __init__(self, db_session: AsyncSession):
        """
        Initialize the rebuttal service.
        
        Args:
            db_session: SQLAlchemy async database session
        """
        self.db = db_session
        self.groq = _get_groq_client()
    
    async def poll_for_unprocessed_content(self, limit: int = 10) -> List:
        """
        Poll the database for unprocessed harmful content.
        
        Args:
            limit: Maximum number of items to fetch
            
        Returns:
            List of Content objects that need processing
        """
        try:
            # Import here to avoid circular imports
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "analyse"))
            from app.models import Content
            from app.database import async_session
            
            async with async_session() as session:
                query = select(Content).where(
                    (Content.is_processed == False) & 
                    (Content.content_type == "harmful")
                ).order_by(Content.created_at.asc()).limit(limit)
                
                result = await session.execute(query)
                items = result.scalars().all()
                logger.info(f"Found {len(items)} unprocessed harmful content item(s)")
                return items
        except Exception as e:
            logger.error(f"Failed to poll for unprocessed content: {e}")
            return []
    
    async def generate_rebuttal(self, tweet_content: str, retries: int = 0) -> Optional[str]:
        """
        Generate a rebuttal for harmful content using Groq LLM.
        
        Args:
            tweet_content: The text of the harmful tweet
            retries: Current retry count
            
        Returns:
            Generated rebuttal text or None if failed
        """
        try:
            prompt = f"""You are a respectful, evidence-based counter-argument specialist. A harmful tweet targeting women has been flagged for analysis. 

Tweet: {tweet_content}

Generate a concise, factual rebuttal (2-3 sentences) that:
- Directly addresses the claim
- Uses data or logic where applicable
- Maintains a respectful, non-confrontational tone
- Does NOT platform or repeat the harmful framing

Rebuttal:"""
            
            response = await self.groq.chat.completions.create(
                model=settings.groq_model,
                messages=[
                    {"role": "system", "content": "You are an expert at generating respectful, evidence-based rebuttals to harmful content."},
                    {"role": "user", "content": prompt}
                ],
                timeout=settings.llm_rebuttal_timeout,
                temperature=0.7,
                max_tokens=300
            )
            
            rebuttal = response.choices[0].message.content.strip()
            logger.info(f"Generated rebuttal (length: {len(rebuttal)} chars)")
            return rebuttal
            
        except Exception as e:
            if retries < settings.rebuttal_max_retries:
                logger.warning(f"Failed to generate rebuttal (attempt {retries + 1}): {e}. Retrying...")
                return await self.generate_rebuttal(tweet_content, retries + 1)
            else:
                logger.error(f"Failed to generate rebuttal after {settings.rebuttal_max_retries} retries: {e}")
                return None
    
    async def update_content_with_rebuttal(
        self, 
        content_id: str, 
        rebuttal: str, 
        twitter_id: str
    ) -> bool:
        """
        Update database record with generated rebuttal.
        
        Args:
            content_id: Internal UUID of the content record
            rebuttal: Generated rebuttal text
            twitter_id: Twitter post ID (for logging)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "analyse"))
            from app.models import Content
            from app.database import async_session
            
            async with async_session() as session:
                # Fetch the content record
                query = select(Content).where(Content.id == content_id)
                result = await session.execute(query)
                content = result.scalars().first()
                
                if not content:
                    logger.error(f"Content record not found: {content_id}")
                    return False
                
                # Update the record
                content.review_comment = rebuttal
                content.is_processed = True
                
                # Append to processing history
                current_history = content.processing_history or []
                history_entry = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "action": "rebuttal_generated",
                    "rebuttal": rebuttal
                }
                current_history.append(history_entry)
                content.processing_history = current_history
                
                # Commit changes
                session.add(content)
                await session.commit()
                
                logger.info(f"Updated content record {content_id} with rebuttal")
                return True
                
        except Exception as e:
            logger.error(f"Failed to update content record {content_id}: {e}")
            return False
    
    async def process_single_item(self, content_item) -> bool:
        """
        Process a single content item: generate rebuttal and update database.
        
        Args:
            content_item: Content ORM object
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Processing content {content_item.twitter_id}")
            
            # Generate rebuttal
            rebuttal = await self.generate_rebuttal(content_item.content_text)
            if not rebuttal:
                logger.error(f"Failed to generate rebuttal for {content_item.twitter_id}")
                return False
            
            # Update database
            success = await self.update_content_with_rebuttal(
                content_item.id,
                rebuttal,
                content_item.twitter_id
            )
            
            if success:
                logger.info(f"Successfully processed content {content_item.twitter_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing content {content_item.twitter_id}: {e}")
            return False
