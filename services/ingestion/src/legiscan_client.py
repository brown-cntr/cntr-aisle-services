import re
import urllib.request
import urllib.parse
import json
import time
import logging
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, date

from shared.utils.config import get_settings
from shared.models.bill import Bill

from .parser import parse_bill_data

logger = logging.getLogger(__name__)


class LegiScanClient:
    """Client for interacting with LegiScan API"""
    
    MAX_RETRIES = 3
    
    AI_SEARCH_QUERY = (
        "(digital NEAR replica) OR (computer-generated) OR (digital NEAR forger) OR "
        "(artificial NEAR intelligence) OR (automated NEAR decision NEAR making) OR "
        "(automatic NEAR decision NEAR making) OR (decision NEAR making NEAR tool) OR "
        "(automated NEAR decision NEAR tool) OR (automatic NEAR decision NEAR tool) OR "
        "(automated NEAR decision NEAR system) OR (automatic NEAR decision NEAR system) OR "
        "(automated NEAR final NEAR decision) OR (automatic NEAR final NEAR decision) OR "
        "(face NEAR recog) OR (facial NEAR recog) OR (voice NEAR recog) OR "
        "(iris NEAR recog) OR (gait NEAR recog) OR (genAI) OR (gen-AI) OR "
        "(generative NEAR AI) OR (generative NEAR tech) OR (generative NEAR model) OR "
        "(generative NEAR artificial) OR (machine NEAR learning) OR (deep NEAR learning) OR "
        "(chat NEAR bot) OR (virtual NEAR assistant) OR (ChatGPT) OR (Chat-GPT) OR "
        "(language NEAR model) OR (AI NEAR task NEAR force) OR (AI NEAR advis) OR "
        "(AI NEAR audit) OR (AI NEAR generate) OR (AI NEAR snoop) OR (deep NEAR fake) OR "
        "(synthetic NEAR media) OR (digital NEAR assistant) OR (natural NEAR language NEAR process) OR "
        "(computer NEAR vision) OR (frontier NEAR model) OR (software NEAR agent) OR "
        "(embodied NEAR robot) OR (foundation NEAR model) OR (LLM) OR (LLMs) OR "
        "(Information NEAR Technology NEAR Act)"
    )
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize LegiScan client"""
        settings = get_settings()
        self.api_key = api_key or getattr(settings, 'legiscan_api_key', None)
        if not self.api_key:
            import os
            self.api_key = os.getenv("LEGISCAN_API_KEY")
        if not self.api_key:
            raise ValueError("LEGISCAN_API_KEY not found in environment or settings")
        
        self.base_url = "https://api.legiscan.com"
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms between requests to avoid rate limiting
    
    def _make_request(self, operation: str, _retries: int = 0, **params) -> Dict[str, Any]:
        """
        Make a request to LegiScan API with rate limiting
        
        Args:
            operation: API operation name (e.g., 'getBill', 'getSearchRaw')
            _retries: Internal retry counter (do not set manually)
            **params: Additional query parameters
            
        Returns:
            API response as dictionary
        """
        # Rate limiting: ensure minimum time between requests
        time_since_last = time.time() - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        
        url = f"{self.base_url}/"
        params_dict = {
            "key": self.api_key,
            "op": operation,
            **params
        }
        
        query_string = urllib.parse.urlencode(params_dict)
        full_url = f"{url}?{query_string}"
        
        try:
            logger.debug(f"Making API request: {operation} with params: {params}")
            response = urllib.request.urlopen(full_url, timeout=30)
            data = json.loads(response.read())
            
            self.request_count += 1
            self.last_request_time = time.time()
            
            # Check for API errors
            if data.get("status") != "OK":
                error_msg = data.get("alert", {}).get("message", "Unknown error")
                logger.error(f"LegiScan API error: {error_msg}")
                raise Exception(f"LegiScan API error: {error_msg}")
            
            return data
            
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if _retries >= self.MAX_RETRIES:
                    raise Exception("Rate limit exceeded after max retries")
                wait = 60 * (2 ** _retries)
                logger.warning(f"Rate limit hit, waiting {wait}s (retry {_retries + 1}/{self.MAX_RETRIES})...")
                time.sleep(wait)
                return self._make_request(operation, _retries=_retries + 1, **params)
            logger.error(f"HTTP Error {e.code}: {e.reason}")
            raise Exception(f"HTTP Error {e.code}: {e.reason}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            raise Exception(f"JSON decode error: {e}")
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def search_ai_bills(
        self, 
        min_relevance: int = 0, 
        state: str = "ALL", 
        use_raw: bool = True
    ) -> tuple[List[Dict], Dict]:
        """
        Search for AI-related bills using LegiScan API
        
        Args:
            min_relevance: Minimum relevance score (0-100)
            state: State code (e.g., "CA", "NY") or "ALL" for all states
            use_raw: If True, use getSearchRaw (up to 2000 results), 
                    else use getSearch (formatted, paginated)
        
        Returns:
            Tuple of (filtered_results, summary)
        """
        operation = "getSearchRaw" if use_raw else "getSearch"
        
        params = {
            "query": self.AI_SEARCH_QUERY,
            "state": state
        }
        
        logger.info(f"Searching LegiScan API with {operation}...")
        logger.debug(f"Query: {self.AI_SEARCH_QUERY[:100]}...")
        
        data = self._make_request(operation, **params)
        
        # Extract results from response
        search_result = data.get("searchresult", {})
        results = search_result.get("results", [])
        summary = search_result.get("summary", {})
        
        logger.info(f"Found {summary.get('count', 0)} total results")
        logger.debug(f"Relevance range: {summary.get('relevancy', 'N/A')}")
        
        # Filter by minimum relevance score
        filtered_results = [
            bill for bill in results 
            if bill.get("relevance", 0) >= min_relevance
        ]
        
        logger.info(f"Filtered to {len(filtered_results)} bills with relevance >= {min_relevance}")
        
        return filtered_results, summary
    
    def get_bill(self, bill_id: int) -> Dict[str, Any]:
        """
        Get detailed bill information
        
        Args:
            bill_id: LegiScan bill ID
            
        Returns:
            Bill data dictionary
        """
        logger.debug(f"Fetching bill {bill_id}...")
        data = self._make_request("getBill", id=bill_id)
        return data.get("bill", {})
    
    def get_bill_text(self, text_id: int) -> Dict[str, Any]:
        """
        Get bill text document
        
        Args:
            text_id: LegiScan text document ID
            
        Returns:
            Text data dictionary
        """
        logger.debug(f"Fetching bill text {text_id}...")
        data = self._make_request("getBillText", id=text_id)
        return data.get("text", {})
    
    def get_bills_from_search_results(
        self,
        search_results: List[Dict[str, Any]],
        existing_legiscan_ids: Optional[Set[int]] = None,
    ) -> List[Bill]:
        """
        Fetch full bill metadata for each search result and parse into Bill models.
        Skips getBill for bill_ids that are in existing_legiscan_ids (already in DB).

        Args:
            search_results: List of search result dictionaries from search_ai_bills
            existing_legiscan_ids: If set, skip getBill for these LegiScan bill IDs

        Returns:
            List of Bill model instances
        """
        bills = []
        skipped = 0

        for i, result in enumerate(search_results, 1):
            raw_bill_id = result.get('bill_id')
            relevance = result.get('relevance', 0)

            if raw_bill_id is None:
                logger.warning(f"Skipping result {i}: no bill_id")
                continue
            try:
                bill_id = int(raw_bill_id)
            except (TypeError, ValueError):
                logger.warning(f"Skipping result {i}: invalid bill_id {raw_bill_id!r}")
                continue

            if existing_legiscan_ids is not None and bill_id in existing_legiscan_ids:
                logger.debug(f"Skipping bill {bill_id} ({i}/{len(search_results)}) - already in database")
                skipped += 1
                continue

            try:
                logger.info(f"Fetching bill {bill_id} ({i}/{len(search_results)}) - relevance: {relevance}")
                bill_data = self.get_bill(bill_id)
                
                if not bill_data:
                    logger.warning(f"No data returned for bill {bill_id}")
                    continue
                
                # Add relevance score to bill data
                bill_data['relevance'] = relevance
                
                # Parse into Bill model
                bill = parse_bill_data(bill_data)
                bills.append(bill)
                
            except Exception as e:
                logger.error(f"Error fetching bill {bill_id}: {e}")
                continue

        if skipped:
            logger.info(f"Skipped {skipped} getBill calls (bills already in database)")
        logger.info(f"Successfully fetched {len(bills)} bills out of {len(search_results)} search results")
        return bills
