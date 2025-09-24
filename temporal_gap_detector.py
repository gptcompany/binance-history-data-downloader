#!/usr/bin/env python3
"""
Temporal Gap Detection System for Binance History Data Downloader

This module provides comprehensive analysis of temporal gaps in downloaded
cryptocurrency data, distinguishing between expected gaps (weekends, holidays)
and problematic missing data that requires attention.
"""

import logging
import csv
import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import calendar


class GapType(Enum):
    """Types of temporal gaps in data"""
    MISSING_DATA = "missing_data"  # Problematic - data should exist but is missing
    WEEKEND_GAP = "weekend_gap"    # Expected - markets closed on weekends
    HOLIDAY_GAP = "holiday_gap"    # Expected - markets closed on holidays
    MARKET_CLOSED = "market_closed"  # Expected - outside trading hours
    API_LIMITATION = "api_limitation"  # Expected - data not available from API
    UNKNOWN_GAP = "unknown_gap"    # Needs investigation


class GapSeverity(Enum):
    """Severity levels for temporal gaps"""
    CRITICAL = "critical"    # Major data gaps affecting analysis
    HIGH = "high"           # Significant gaps needing attention
    MEDIUM = "medium"       # Moderate gaps, investigate when possible
    LOW = "low"             # Minor gaps, expected behavior
    INFO = "info"           # Informational, no action needed


@dataclass
class GapInfo:
    """Information about a detected temporal gap"""
    gap_type: GapType
    severity: GapSeverity
    start_date: date
    end_date: date
    duration_days: int
    symbol: str
    data_type: str
    interval: Optional[str]
    expected_files: List[str]
    missing_files: List[str]
    context: Dict[str, Any]
    detected_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = asdict(self)
        result['gap_type'] = self.gap_type.value
        result['severity'] = self.severity.value
        result['start_date'] = self.start_date.isoformat()
        result['end_date'] = self.end_date.isoformat()
        result['detected_at'] = self.detected_at.isoformat()
        return result


@dataclass
class TimelineAnalysisResult:
    """Result of timeline continuity analysis"""
    symbol: str
    data_type: str
    interval: Optional[str]
    analysis_period_start: date
    analysis_period_end: date
    total_expected_files: int
    total_existing_files: int
    total_missing_files: int
    completeness_percentage: float
    gaps: List[GapInfo]
    recommendations: List[str]


class MarketCalendar:
    """Knowledge about market closures and expected gaps"""

    @staticmethod
    def is_weekend(check_date: date) -> bool:
        """Check if date falls on weekend (crypto markets trade 24/7 but some data may be limited)"""
        return check_date.weekday() >= 5  # Saturday = 5, Sunday = 6

    @staticmethod
    def is_known_holiday(check_date: date) -> bool:
        """Check if date is a known market holiday"""
        # For crypto markets, there are generally no holidays
        # but this can be extended for traditional market integration
        return False

    @staticmethod
    def get_expected_gap_type(check_date: date, data_type: str) -> Optional[GapType]:
        """Determine if a gap on this date is expected"""
        if MarketCalendar.is_weekend(check_date):
            # Most crypto data should be available on weekends
            # but some aggregated metrics might have gaps
            if data_type in ['metrics', 'fundingRate']:
                return GapType.WEEKEND_GAP
            return None  # Crypto markets operate on weekends

        if MarketCalendar.is_known_holiday(check_date):
            return GapType.HOLIDAY_GAP

        return None


class FilePatternAnalyzer:
    """Analyzes file naming patterns and existence for gap detection"""

    def __init__(self, data_dir: Path, downloads_dir: Path):
        self.data_dir = data_dir
        self.downloads_dir = downloads_dir
        self.logger = logging.getLogger("downloader.gap_detector.file_analyzer")

    def get_existing_files(self, symbol: str, data_type: str, interval: Optional[str] = None) -> Dict[date, Path]:
        """Get mapping of dates to existing files for a symbol/data_type combination"""
        existing_files = {}

        # Determine search pattern based on data type
        if data_type == "fundingRate":
            # Monthly files: symbol-datatype-YYYY-MM.csv
            search_pattern = f"{symbol}-{data_type}-*.csv"
            search_dir = self.data_dir / "monthly" / data_type / symbol
        elif interval:
            # Daily files with interval: symbol-interval-YYYY-MM-DD.csv
            search_pattern = f"{symbol}-{interval}-*.csv"
            search_dir = self.data_dir / symbol / data_type / interval
        else:
            # Daily files without interval: symbol-datatype-YYYY-MM-DD.csv
            search_pattern = f"{symbol}-{data_type}-*.csv"
            search_dir = self.data_dir / symbol / data_type

        if not search_dir.exists():
            self.logger.debug(f"Search directory does not exist: {search_dir}")
            return existing_files

        # Find and parse file dates
        for file_path in search_dir.glob(search_pattern):
            try:
                file_date = self._extract_date_from_filename(file_path.name, data_type)
                if file_date:
                    existing_files[file_date] = file_path
            except Exception as e:
                self.logger.warning(f"Could not parse date from file {file_path.name}: {e}")

        return existing_files

    def _extract_date_from_filename(self, filename: str, data_type: str) -> Optional[date]:
        """Extract date from filename based on data type"""
        try:
            # Remove .csv extension
            name_without_ext = filename.replace('.csv', '')

            if data_type == "fundingRate":
                # Format: SYMBOL-fundingRate-YYYY-MM.csv
                parts = name_without_ext.split('-')
                if len(parts) >= 4:
                    year_month = '-'.join(parts[-2:])  # Last two parts: YYYY-MM
                    year, month = year_month.split('-')
                    return date(int(year), int(month), 1)
            else:
                # Daily format: SYMBOL-[interval-]datatype-YYYY-MM-DD.csv
                # or: SYMBOL-interval-YYYY-MM-DD.csv
                parts = name_without_ext.split('-')
                if len(parts) >= 4:
                    date_part = '-'.join(parts[-3:])  # Last three parts: YYYY-MM-DD
                    return datetime.strptime(date_part, '%Y-%m-%d').date()

        except Exception as e:
            self.logger.debug(f"Date extraction failed for {filename}: {e}")

        return None

    def generate_expected_dates(self, start_date: date, end_date: date, data_type: str) -> List[date]:
        """Generate list of expected dates for a data type"""
        expected_dates = []
        current_date = start_date

        if data_type == "fundingRate":
            # Monthly data - first day of each month
            while current_date <= end_date:
                expected_dates.append(current_date.replace(day=1))
                # Move to first day of next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
        else:
            # Daily data
            while current_date <= end_date:
                expected_dates.append(current_date)
                current_date += timedelta(days=1)

        return expected_dates


class TemporalGapDetector:
    """Main class for detecting and analyzing temporal gaps in downloaded data"""

    def __init__(self, data_dir: Path, downloads_dir: Path):
        self.data_dir = Path(data_dir)
        self.downloads_dir = Path(downloads_dir)
        self.file_analyzer = FilePatternAnalyzer(data_dir, downloads_dir)
        self.logger = logging.getLogger("downloader.gap_detector")

        # Configuration
        self.max_acceptable_gap_days = 7  # Gaps longer than this are always concerning
        self.critical_gap_threshold_pct = 20.0  # >20% missing data is critical

    def analyze_symbol_timeline(
        self,
        symbol: str,
        data_type: str,
        start_date: date,
        end_date: date,
        interval: Optional[str] = None
    ) -> TimelineAnalysisResult:
        """Analyze timeline continuity for a specific symbol/data_type combination"""

        self.logger.info(f"Analyzing timeline for {symbol} {data_type} {interval or ''} from {start_date} to {end_date}")

        # Get existing files
        existing_files = self.file_analyzer.get_existing_files(symbol, data_type, interval)
        existing_dates = set(existing_files.keys())

        # Generate expected dates
        expected_dates = self.file_analyzer.generate_expected_dates(start_date, end_date, data_type)
        expected_dates_set = set(expected_dates)

        # Find missing dates
        missing_dates = expected_dates_set - existing_dates
        total_expected = len(expected_dates)
        total_existing = len(existing_dates)
        total_missing = len(missing_dates)

        # Calculate completeness percentage
        completeness_pct = (total_existing / total_expected * 100) if total_expected > 0 else 0

        # Detect and classify gaps
        gaps = self._detect_gaps(missing_dates, symbol, data_type, interval)

        # Generate recommendations
        recommendations = self._generate_recommendations(gaps, completeness_pct, total_missing)

        return TimelineAnalysisResult(
            symbol=symbol,
            data_type=data_type,
            interval=interval,
            analysis_period_start=start_date,
            analysis_period_end=end_date,
            total_expected_files=total_expected,
            total_existing_files=total_existing,
            total_missing_files=total_missing,
            completeness_percentage=completeness_pct,
            gaps=gaps,
            recommendations=recommendations
        )

    def _detect_gaps(self, missing_dates: Set[date], symbol: str, data_type: str, interval: Optional[str]) -> List[GapInfo]:
        """Detect and classify temporal gaps from missing dates"""
        if not missing_dates:
            return []

        gaps = []
        sorted_missing = sorted(missing_dates)

        # Group consecutive missing dates into gap periods
        gap_groups = self._group_consecutive_dates(sorted_missing)

        for group in gap_groups:
            start_date = min(group)
            end_date = max(group)
            duration = (end_date - start_date).days + 1

            # Classify gap type and severity
            gap_type = self._classify_gap_type(group, data_type)
            severity = self._assess_gap_severity(gap_type, duration, len(group))

            # Generate expected and missing file names
            expected_files = self._generate_expected_filenames(group, symbol, data_type, interval)
            missing_files = expected_files.copy()  # All expected files are missing in this gap

            context = {
                "consecutive_days": len(group),
                "percentage_of_group": 100.0,  # All days in this group are missing
                "analysis_notes": self._get_gap_analysis_notes(gap_type, duration, group)
            }

            gap_info = GapInfo(
                gap_type=gap_type,
                severity=severity,
                start_date=start_date,
                end_date=end_date,
                duration_days=duration,
                symbol=symbol,
                data_type=data_type,
                interval=interval,
                expected_files=expected_files,
                missing_files=missing_files,
                context=context,
                detected_at=datetime.now()
            )

            gaps.append(gap_info)

        return gaps

    def _group_consecutive_dates(self, sorted_dates: List[date]) -> List[List[date]]:
        """Group consecutive dates together"""
        if not sorted_dates:
            return []

        groups = []
        current_group = [sorted_dates[0]]

        for i in range(1, len(sorted_dates)):
            current_date = sorted_dates[i]
            previous_date = sorted_dates[i-1]

            # Check if dates are consecutive
            if (current_date - previous_date).days == 1:
                current_group.append(current_date)
            else:
                # Start new group
                groups.append(current_group)
                current_group = [current_date]

        # Add the last group
        groups.append(current_group)

        return groups

    def _classify_gap_type(self, missing_date_group: List[date], data_type: str) -> GapType:
        """Classify the type of gap based on dates and context"""
        # Check if all dates in group have expected gap type
        weekend_count = sum(1 for d in missing_date_group if MarketCalendar.is_weekend(d))
        holiday_count = sum(1 for d in missing_date_group if MarketCalendar.is_known_holiday(d))

        total_dates = len(missing_date_group)

        # If majority are weekends, classify as weekend gap
        if weekend_count / total_dates > 0.5:
            return GapType.WEEKEND_GAP

        # If majority are holidays, classify as holiday gap
        if holiday_count / total_dates > 0.5:
            return GapType.HOLIDAY_GAP

        # For funding rate data, some gaps might be due to API limitations
        if data_type == "fundingRate" and total_dates <= 2:
            return GapType.API_LIMITATION

        # Default to missing data
        return GapType.MISSING_DATA

    def _assess_gap_severity(self, gap_type: GapType, duration_days: int, missing_count: int) -> GapSeverity:
        """Assess the severity of a temporal gap"""
        if gap_type in [GapType.WEEKEND_GAP, GapType.HOLIDAY_GAP, GapType.MARKET_CLOSED]:
            return GapSeverity.INFO

        if gap_type == GapType.API_LIMITATION:
            return GapSeverity.LOW

        # For missing data, assess based on duration
        if duration_days >= self.max_acceptable_gap_days:
            return GapSeverity.CRITICAL
        elif duration_days >= 3:
            return GapSeverity.HIGH
        elif duration_days >= 1:
            return GapSeverity.MEDIUM
        else:
            return GapSeverity.LOW

    def _generate_expected_filenames(self, dates: List[date], symbol: str, data_type: str, interval: Optional[str]) -> List[str]:
        """Generate expected filenames for missing dates"""
        filenames = []

        for date_obj in dates:
            if data_type == "fundingRate":
                # Monthly format: symbol-datatype-YYYY-MM.csv
                filename = f"{symbol}-{data_type}-{date_obj.strftime('%Y-%m')}.csv"
            elif interval:
                # Daily with interval: symbol-interval-YYYY-MM-DD.csv
                filename = f"{symbol}-{interval}-{date_obj.strftime('%Y-%m-%d')}.csv"
            else:
                # Daily without interval: symbol-datatype-YYYY-MM-DD.csv
                filename = f"{symbol}-{data_type}-{date_obj.strftime('%Y-%m-%d')}.csv"

            filenames.append(filename)

        return filenames

    def _get_gap_analysis_notes(self, gap_type: GapType, duration_days: int, missing_dates: List[date]) -> str:
        """Generate analysis notes for a gap"""
        notes = []

        if gap_type == GapType.MISSING_DATA:
            if duration_days > 7:
                notes.append("Extended gap requiring immediate attention")
            elif duration_days > 3:
                notes.append("Significant gap affecting data continuity")
            else:
                notes.append("Short gap, may be due to temporary issues")

        elif gap_type == GapType.WEEKEND_GAP:
            notes.append("Weekend gap - expected for some data types")

        elif gap_type == GapType.API_LIMITATION:
            notes.append("Possible API limitation or data availability issue")

        # Add date range context
        if len(missing_dates) > 1:
            start_date = min(missing_dates)
            end_date = max(missing_dates)
            notes.append(f"Gap spans from {start_date} to {end_date}")

        return "; ".join(notes)

    def _generate_recommendations(self, gaps: List[GapInfo], completeness_pct: float, total_missing: int) -> List[str]:
        """Generate actionable recommendations based on gap analysis"""
        recommendations = []

        # Overall completeness assessment
        if completeness_pct < 50:
            recommendations.append("CRITICAL: Less than 50% data completeness - requires immediate attention")
        elif completeness_pct < 80:
            recommendations.append("WARNING: Data completeness below 80% - investigate missing data sources")
        elif completeness_pct < 95:
            recommendations.append("INFO: Some data gaps detected - consider re-running downloads for missing periods")

        # Critical gaps
        critical_gaps = [g for g in gaps if g.severity == GapSeverity.CRITICAL]
        if critical_gaps:
            recommendations.append(f"URGENT: {len(critical_gaps)} critical gap(s) detected requiring immediate re-download")

        # High severity gaps
        high_gaps = [g for g in gaps if g.severity == GapSeverity.HIGH]
        if high_gaps:
            recommendations.append(f"PRIORITY: {len(high_gaps)} high-priority gap(s) should be addressed soon")

        # Pattern analysis
        missing_data_gaps = [g for g in gaps if g.gap_type == GapType.MISSING_DATA]
        if len(missing_data_gaps) > 5:
            recommendations.append("PATTERN: Multiple missing data gaps detected - check download configuration and API limits")

        # Specific actionable recommendations
        if total_missing > 0:
            recommendations.append("ACTION: Use gap report to prioritize re-download of missing files")

        return recommendations

    def generate_comprehensive_report(self, analysis_results: List[TimelineAnalysisResult], output_dir: Path) -> Path:
        """Generate comprehensive gap analysis report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = output_dir / f"temporal_gap_analysis_{timestamp}.json"

        # Prepare report data
        report_data = {
            "analysis_metadata": {
                "generated_at": datetime.now().isoformat(),
                "analyzed_combinations": len(analysis_results),
                "report_version": "1.0"
            },
            "executive_summary": self._generate_executive_summary(analysis_results),
            "detailed_analysis": [self._analysis_result_to_dict(result) for result in analysis_results]
        }

        # Write JSON report
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        # Also generate CSV summary for easy viewing
        csv_path = self._generate_csv_summary(analysis_results, output_dir, timestamp)

        self.logger.info(f"Temporal gap analysis report generated: {report_path}")
        self.logger.info(f"Gap summary CSV generated: {csv_path}")

        return report_path

    def _generate_executive_summary(self, analysis_results: List[TimelineAnalysisResult]) -> Dict[str, Any]:
        """Generate executive summary of all analysis results"""
        total_combinations = len(analysis_results)
        total_gaps = sum(len(result.gaps) for result in analysis_results)

        # Completeness statistics
        completeness_values = [result.completeness_percentage for result in analysis_results]
        avg_completeness = sum(completeness_values) / len(completeness_values) if completeness_values else 0

        # Severity breakdown
        severity_counts = {severity.value: 0 for severity in GapSeverity}
        gap_type_counts = {gap_type.value: 0 for gap_type in GapType}

        for result in analysis_results:
            for gap in result.gaps:
                severity_counts[gap.severity.value] += 1
                gap_type_counts[gap.gap_type.value] += 1

        return {
            "total_analyzed_combinations": total_combinations,
            "average_completeness_percentage": round(avg_completeness, 2),
            "total_gaps_detected": total_gaps,
            "severity_breakdown": severity_counts,
            "gap_type_breakdown": gap_type_counts,
            "critical_combinations": len([r for r in analysis_results if r.completeness_percentage < 50]),
            "healthy_combinations": len([r for r in analysis_results if r.completeness_percentage >= 95])
        }

    def _analysis_result_to_dict(self, result: TimelineAnalysisResult) -> Dict[str, Any]:
        """Convert TimelineAnalysisResult to dictionary for JSON serialization"""
        return {
            "symbol": result.symbol,
            "data_type": result.data_type,
            "interval": result.interval,
            "analysis_period": {
                "start": result.analysis_period_start.isoformat(),
                "end": result.analysis_period_end.isoformat()
            },
            "statistics": {
                "total_expected_files": result.total_expected_files,
                "total_existing_files": result.total_existing_files,
                "total_missing_files": result.total_missing_files,
                "completeness_percentage": result.completeness_percentage
            },
            "gaps": [gap.to_dict() for gap in result.gaps],
            "recommendations": result.recommendations
        }

    def _generate_csv_summary(self, analysis_results: List[TimelineAnalysisResult], output_dir: Path, timestamp: str) -> Path:
        """Generate CSV summary of gap analysis for easy viewing"""
        csv_path = output_dir / f"gap_summary_{timestamp}.csv"

        fieldnames = [
            'symbol', 'data_type', 'interval', 'completeness_pct',
            'total_expected', 'total_existing', 'total_missing',
            'critical_gaps', 'high_gaps', 'medium_gaps', 'low_gaps', 'info_gaps',
            'recommendations_count', 'needs_attention'
        ]

        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for result in analysis_results:
                # Count gaps by severity
                gap_counts = {severity.value: 0 for severity in GapSeverity}
                for gap in result.gaps:
                    gap_counts[gap.severity.value] += 1

                row = {
                    'symbol': result.symbol,
                    'data_type': result.data_type,
                    'interval': result.interval or '',
                    'completeness_pct': round(result.completeness_percentage, 2),
                    'total_expected': result.total_expected_files,
                    'total_existing': result.total_existing_files,
                    'total_missing': result.total_missing_files,
                    'critical_gaps': gap_counts['critical'],
                    'high_gaps': gap_counts['high'],
                    'medium_gaps': gap_counts['medium'],
                    'low_gaps': gap_counts['low'],
                    'info_gaps': gap_counts['info'],
                    'recommendations_count': len(result.recommendations),
                    'needs_attention': 'YES' if result.completeness_percentage < 95 or gap_counts['critical'] > 0 else 'NO'
                }

                writer.writerow(row)

        return csv_path


# Convenience function for quick gap analysis
def analyze_temporal_gaps(
    symbols: List[str],
    data_types: List[str],
    start_date: date,
    end_date: date,
    data_dir: Path = Path("data"),
    downloads_dir: Path = Path("downloads"),
    intervals: Optional[Dict[str, str]] = None  # data_type -> interval mapping
) -> List[TimelineAnalysisResult]:
    """
    Convenience function to analyze temporal gaps for multiple symbol/data_type combinations

    Args:
        symbols: List of trading symbols to analyze
        data_types: List of data types to analyze
        start_date: Analysis period start date
        end_date: Analysis period end date
        data_dir: Directory containing extracted data files
        downloads_dir: Directory containing downloaded ZIP files
        intervals: Optional mapping of data_type to interval (e.g., {'klines': '1m'})

    Returns:
        List of timeline analysis results
    """
    detector = TemporalGapDetector(data_dir, downloads_dir)
    results = []

    for symbol in symbols:
        for data_type in data_types:
            interval = intervals.get(data_type) if intervals else None

            try:
                result = detector.analyze_symbol_timeline(
                    symbol=symbol,
                    data_type=data_type,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval
                )
                results.append(result)
            except Exception as e:
                logging.getLogger("downloader.gap_detector").error(
                    f"Failed to analyze {symbol} {data_type} {interval}: {e}"
                )

    return results