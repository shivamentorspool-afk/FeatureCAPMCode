using com.datasphere.featureanalytic as db from '../db/schema';

// ─────────────────────────────────────────────
// Feature Analytic OData Service
// Exposed at /odata/v4/FeatureAnalyticService
// ─────────────────────────────────────────────
service FeatureAnalyticService @(path: '/odata/v4/FeatureAnalyticService') {

    // ── Main data entity - read + update ─────
    @Capabilities.UpdateRestrictions.Updatable: true
    @Capabilities.DeleteRestrictions.Deletable: false
    @Capabilities.InsertRestrictions.Insertable: false
    entity Feature_Analytic_Model as projection on db.Feature_Analytic_Model;

    // ── Sync log for monitoring ───────────────
    @readonly
    entity SyncLog as projection on db.SyncLog;

    // ── Update log for audit trail ────────────
    @readonly
    entity UpdateLog as projection on db.UpdateLog;

    // ── Action: update feature status ─────────
    // Sends PATCH to SAP Cloud ALM API and refreshes cache
    action updateFeatureStatus(
        featureId  : String,
        newStatus  : String,
        updatedBy  : String
    ) returns {
        success    : Boolean;
        message    : String;
        featureId  : String;
        oldStatus  : String;
        newStatus  : String;
        updateTime : Timestamp;
    };

    // ── Action: manually trigger data refresh ─
    action triggerRefresh() returns {
        success    : Boolean;
        message    : String;
        count      : Integer;
        pages      : Integer;
        syncTime   : Timestamp;
    };

    // ── Function: get current sync status ─────
    function getSyncStatus() returns {
        lastSync    : Timestamp;
        recordCount : Integer;
        nextSync    : Timestamp;
        status      : String;
        pagesFetched: Integer;
    };

    // ── Function: get valid status values ─────
    function getValidStatuses() returns array of {
        code        : String;
        description : String;
    };
}
