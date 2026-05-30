"""
ML Analytics Dashboard for UTSF Enhancement
==============================================
Real-time analytics and monitoring for ML-powered data enhancement.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict, Counter

# Optional matplotlib for visualization
try:
    import matplotlib.pyplot as plt
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    # Create minimal numpy-like functions
    class MockNumpy:
        @staticmethod
        def mean(data):
            return sum(data) / len(data) if data else 0
        @staticmethod
        def median(data):
            sorted_data = sorted(data)
            n = len(sorted_data)
            if n == 0:
                return 0
            elif n % 2 == 0:
                return (sorted_data[n//2-1] + sorted_data[n//2]) / 2
            else:
                return sorted_data[n//2]
    np = MockNumpy()

class MLEnhancementAnalytics:
    """Analytics dashboard for ML enhancement performance."""
    
    def __init__(self):
        self.enhancement_log = []
        self.performance_metrics = defaultdict(list)
        self.confidence_distribution = []
        self.enhancement_types_counter = Counter()
        
    def log_enhancement(self, transporter_name: str, enhancements: List[str], 
                      confidence: float, before_quality: float, after_quality: float):
        """Log an enhancement event for analytics."""
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "transporter": transporter_name,
            "enhancements": enhancements,
            "confidence": confidence,
            "before_quality": before_quality,
            "after_quality": after_quality,
            "improvement": after_quality - before_quality
        }
        
        self.enhancement_log.append(event)
        self.confidence_distribution.append(confidence)
        self.enhancement_types_counter.update(enhancements)
        
        # Track performance metrics
        self.performance_metrics["quality_improvement"].append(event["improvement"])
        self.performance_metrics["confidence_score"].append(confidence)
        
    def get_enhancement_summary(self) -> Dict:
        """Get comprehensive enhancement summary."""
        if not self.enhancement_log:
            return {"status": "No enhancements logged yet"}
        
        total_enhancements = len(self.enhancement_log)
        avg_improvement = np.mean([e["improvement"] for e in self.enhancement_log])
        avg_confidence = np.mean(self.confidence_distribution)
        
        # Quality distribution
        quality_before = [e["before_quality"] for e in self.enhancement_log]
        quality_after = [e["after_quality"] for e in self.enhancement_log]
        
        # Enhancement effectiveness
        successful_enhancements = [e for e in self.enhancement_log if e["improvement"] > 0]
        success_rate = len(successful_enhancements) / total_enhancements * 100
        
        return {
            "total_enhancements": total_enhancements,
            "average_quality_improvement": round(avg_improvement, 2),
            "average_confidence": round(avg_confidence, 2),
            "success_rate": round(success_rate, 2),
            "quality_before_avg": round(np.mean(quality_before), 2),
            "quality_after_avg": round(np.mean(quality_after), 2),
            "most_common_enhancements": dict(self.enhancement_types_counter.most_common(5)),
            "recent_enhancements": self.enhancement_log[-5:],
            "performance_breakdown": self._get_performance_breakdown()
        }
    
    def _get_performance_breakdown(self) -> Dict:
        """Get detailed performance breakdown by enhancement type."""
        type_performance = defaultdict(lambda: {"improvements": [], "confidences": []})
        
        for event in self.enhancement_log:
            for enhancement_type in event["enhancements"]:
                type_performance[enhancement_type]["improvements"].append(event["improvement"])
                type_performance[enhancement_type]["confidences"].append(event["confidence"])
        
        breakdown = {}
        for enhancement_type, data in type_performance.items():
            breakdown[enhancement_type] = {
                "count": len(data["improvements"]),
                "avg_improvement": round(np.mean(data["improvements"]), 2),
                "avg_confidence": round(np.mean(data["confidences"]), 2),
                "effectiveness": round(np.mean([i for i in data["improvements"] if i > 0]) * 100, 2)
            }
        
        return breakdown
    
    def generate_ml_report(self) -> str:
        """Generate comprehensive ML performance report."""
        summary = self.get_enhancement_summary()
        
        report = f"""
# UTSF ML Enhancement Performance Report
Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

## Executive Summary
- **Total Enhancements**: {summary['total_enhancements']}
- **Average Quality Improvement**: {summary['average_quality_improvement']} points
- **Success Rate**: {summary['success_rate']}%
- **Average Confidence**: {summary['average_confidence']}

## Quality Metrics
- **Before Enhancement (Avg)**: {summary['quality_before_avg']}/100
- **After Enhancement (Avg)**: {summary['quality_after_avg']}/100
- **Net Improvement**: {summary['average_quality_improvement']} points

## Most Effective Enhancements
"""
        
        for enhancement, stats in summary['most_common_enhancements'].items():
            report += f"- **{enhancement}**: {stats} applications\n"
        
        report += "\n## Performance by Enhancement Type\n"
        for enhancement_type, perf in summary['performance_breakdown'].items():
            report += f"""
### {enhancement_type}
- Applications: {perf['count']}
- Avg Improvement: {perf['avg_improvement']} points
- Avg Confidence: {perf['avg_confidence']}
- Effectiveness: {perf['effectiveness']}%
"""
        
        report += "\n## Recent Activity\n"
        for event in summary['recent_enhancements'][-3:]:
            report += f"- {event['transporter']}: {event['improvement']:+.1f} points (confidence: {event['confidence']:.2f})\n"
        
        return report
    
    def predict_enhancement_potential(self, raw_data: Dict) -> Dict:
        """Predict enhancement potential for given raw data."""
        # Analyze data completeness
        completeness_score = self._calculate_data_completeness(raw_data)
        
        # Predict likely enhancements
        likely_enhancements = []
        
        if not raw_data.get("company_details", {}).get("gstNo"):
            likely_enhancements.append("gst_ml")
        
        if not raw_data.get("company_details", {}).get("phone"):
            likely_enhancements.append("contact_ml")
        
        if not raw_data.get("zone_matrix") and raw_data.get("charges", {}).get("odaCharges"):
            likely_enhancements.append("zone_rates_ml")
        
        if completeness_score < 0.8:
            likely_enhancements.append("business_logic_ml")
        
        # Predict quality improvement
        base_quality = completeness_score * 100
        predicted_improvement = len(likely_enhancements) * 15  # ~15 points per enhancement
        predicted_quality = min(base_quality + predicted_improvement, 100)
        
        return {
            "current_completeness": round(completeness_score, 2),
            "likely_enhancements": likely_enhancements,
            "predicted_improvement": round(predicted_improvement, 1),
            "predicted_quality": round(predicted_quality, 1),
            "enhancement_potential": "High" if predicted_improvement > 30 else "Medium" if predicted_improvement > 15 else "Low"
        }
    
    def _calculate_data_completeness(self, data: Dict) -> float:
        """Calculate data completeness score."""
        required_sections = ["company_details", "charges", "zone_matrix", "served_pincodes"]
        present_sections = sum(1 for section in required_sections if data.get(section))
        return present_sections / len(required_sections)

# Global analytics instance - shared across all modules
ml_analytics = MLEnhancementAnalytics()

def log_ml_enhancement(transporter_name: str, enhancements: List[str], 
                     confidence: float, before_quality: float, after_quality: float):
    """Log ML enhancement for analytics."""
    ml_analytics.log_enhancement(transporter_name, enhancements, confidence, before_quality, after_quality)

def get_ml_analytics() -> Dict:
    """Get current ML analytics."""
    return ml_analytics.get_enhancement_summary()

def generate_ml_performance_report() -> str:
    """Generate ML performance report."""
    return ml_analytics.generate_ml_report()

# Test function to verify analytics are working
def test_analytics():
    """Test analytics functionality."""
    log_ml_enhancement("system_test", ["test_ml"], 0.9, 50.0, 80.0)
    return get_ml_analytics()
