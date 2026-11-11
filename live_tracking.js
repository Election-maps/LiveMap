/**
 * Live Forces Real-time Tracking (Routes, SF, SSF, QRTs, Reserve Parties)
 * Exclusively active on "Live Route Map" tab (liveMap)
 * Srikakulam District Police
 */

window.livePartyMarkers = {};
window.livePartyMarkersGroup = null;
window.currentLivePartyCategory = 'ALL';
window.liveMqttClient = null;

const LIVE_MQTT_BROKER = 'wss://broker.emqx.io:8084/mqtt';
const LIVE_ROOM_CODE = 'SrikakulamPolice';
const liveUserId = 'control_room_gp_' + Math.random().toString(36).substring(2, 9);

// Forces metadata lookup from route_mobiles_data.js
function getPartyUnitMeta(unitId) {
    if (!window.ROUTE_MOBILES_DATA || !Array.isArray(window.ROUTE_MOBILES_DATA)) return null;
    const cleanId = String(unitId || '').trim().toLowerCase();
    return window.ROUTE_MOBILES_DATA.find(function(u) {
        if (!u) return false;
        if (String(u.id || '').trim().toLowerCase() === cleanId) return true;
        if (String(u.unitId || '').trim().toLowerCase() === cleanId) return true;
        return false;
    }) || null;
}

function getLivePartyCategoryTheme(catKey) {
    switch (String(catKey || '').toUpperCase()) {
        case 'SF':
            return { color: '#ef4444', icon: 'fa-shield-halved', label: 'Striking Force', badgeClass: 'bg-rose-100 text-rose-800 border-rose-300' };
        case 'SSF':
            return { color: '#a855f7', icon: 'fa-user-shield', label: 'Special Striking Force', badgeClass: 'bg-purple-100 text-purple-800 border-purple-300' };
        case 'QRT':
            return { color: '#f59e0b', icon: 'fa-bolt', label: 'QRT', badgeClass: 'bg-amber-100 text-amber-800 border-amber-300' };
        case 'RESERVE':
            return { color: '#10b981', icon: 'fa-building-columns', label: 'Reserve Party', badgeClass: 'bg-emerald-100 text-emerald-800 border-emerald-300' };
        default:
            return { color: '#3b82f6', icon: 'fa-car-side', label: 'Route Mobile', badgeClass: 'bg-blue-100 text-blue-800 border-blue-300' };
    }
}

window.initLivePartyTracking = function() {
    if (typeof liveMap === 'undefined' || !liveMap) {
        setTimeout(window.initLivePartyTracking, 200);
        return;
    }

    if (!window.livePartyMarkersGroup) {
        window.livePartyMarkersGroup = L.layerGroup().addTo(liveMap);
    }

    restoreLiveUnitsFromCache();
    initOfficerMessagesStorage();
    initLiveMQTT();
};

function initLiveMQTT() {
    if (window.liveMqttClient && window.liveMqttClient.connected) return;
    if (typeof mqtt === 'undefined') {
        setTimeout(initLiveMQTT, 300);
        return;
    }

    try {
        window.liveMqttClient = mqtt.connect(LIVE_MQTT_BROKER, {
            clientId: 'gp_cr_' + liveUserId + '_' + Math.random().toString(16).substring(2, 8),
            clean: true,
            connectTimeout: 7000,
            reconnectPeriod: 3000,
            keepalive: 25
        });

        window.liveMqttClient.on('connect', function() {
            console.log('Connected to Srikakulam Police Real-time Broker (Live Route Map)!');
            const dot = document.getElementById('live-mqtt-indicator-dot');
            const txt = document.getElementById('live-mqtt-status-text');
            if (dot) dot.className = 'w-2 h-2 rounded-full bg-emerald-500 animate-ping inline-block';
            if (txt) {
                txt.innerText = 'Online';
                txt.className = 'text-emerald-400 font-bold';
            }

            // Subscribe to general locations topic and retained officer topics
            const generalTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/locations';
            const retainedTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/officer/+';
            const controlTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/control_command';
            const officerMsgTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/officer_messages';
            window.liveMqttClient.subscribe(officerMsgTopic, { qos: 1 });
            const historyTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/messages_history';
            window.liveMqttClient.subscribe(historyTopic, { qos: 1 });
            window.liveMqttClient.subscribe(generalTopic, { qos: 0 });
            window.liveMqttClient.subscribe(controlTopic, { qos: 1 });
            window.liveMqttClient.subscribe(retainedTopic, { qos: 1 }, function(err) {
                if (!err) {
                    const syncTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/sync_request';
                    window.liveMqttClient.publish(syncTopic, JSON.stringify({ type: 'sync_request', from: liveUserId }), { qos: 0 });
                }
            });
        });

        window.liveMqttClient.on('message', function(topic, message) {
            try {
                const str = message.toString().trim();
                if (!str) return;
                const payload = JSON.parse(str);

                // Handle remote clear commands
                if (topic.indexOf('/messages_history') !== -1) {
                    handleIncomingMessagesHistory(payload);
                    return;
                }

                if (payload.type === 'sync_request' && payload.from !== liveUserId) {
                    if (Array.isArray(window.liveOfficerMessages) && window.liveOfficerMessages.length > 0) {
                        broadcastMessagesHistory();
                    }
                }

                if (payload.action === 'clear_all') {
                    clearAllLiveTrackingLocally();
                    return;
                }

                // Handle incoming officer messages / SOS alerts
                if (topic.indexOf('/officer_messages') !== -1 || payload.type === 'officer_message' || (payload.message && typeof payload.message === 'string' && payload.message.trim())) {
                    handleIncomingOfficerMessage(payload);
                    if (!payload.latitude && !payload.lat) return;
                }

                const unitKey = payload.unitId || payload.id || topic.split('/').pop();
                if (!unitKey) return;

                if (payload.status === 'stopped' || payload.action === 'clear_unit') {
                    removeLivePartyUnit(unitKey);
                } else if (payload.latitude || payload.lat) {
                    updateLivePartyUnitOnMap(unitKey, payload);
                }
            } catch (e) {
                console.error('Failed to parse live tracking packet:', e);
            }
        });

        window.liveMqttClient.on('error', function(err) {
            console.warn('MQTT Connection Error:', err);
            const txt = document.getElementById('live-mqtt-status-text');
            if (txt) {
                txt.innerText = 'Reconnecting...';
                txt.className = 'text-amber-400 font-bold';
            }
        });

        window.liveMqttClient.on('offline', function() {
            const txt = document.getElementById('live-mqtt-status-text');
            if (txt) {
                txt.innerText = 'Offline';
                txt.className = 'text-rose-400 font-bold';
            }
        });
    } catch (err) {
        console.error('MQTT Init failed:', err);
    }
}

function updateLivePartyUnitOnMap(key, data) {
    if (typeof liveMap === 'undefined' || !liveMap || !window.livePartyMarkersGroup) return;

    const lat = parseFloat(data.latitude || data.lat);
    const lng = parseFloat(data.longitude || data.lng);
    if (isNaN(lat) || isNaN(lng) || lat === 0 || lng === 0) return;

    const meta = getPartyUnitMeta(key) || {};
    const catKey = (data.categoryKey || meta.categoryKey || 'RM').toUpperCase();
    const mandal = data.mandal || meta.mandal || '';
    const unitName = data.unitName || meta.unitName || key;
    const officer = data.officer || meta.officer || data.name || 'Officer';
    const phone = data.phone || meta.phone || '';
    const speed = parseFloat(data.speed || 0);
    const speedKmh = Math.round(speed * 3.6);
    const isMoving = speedKmh >= 2 || !!data.isMoving;
    const bearing = parseFloat(data.bearing || 0);
    const accuracy = parseFloat(data.accuracy || 10);
    const lastUpdated = data.lastUpdated || Date.now();

    let unit = window.livePartyMarkers[key];
    if (!unit) {
        // Create new marker with compact sizing
        const icon = createLivePoliceMarkerIcon(catKey, mandal, unitName, true, isMoving, bearing, speedKmh, officer);
        const marker = L.marker([lat, lng], { icon: icon, zIndexOffset: 1000 });
        
        const circle = L.circle([lat, lng], {
            radius: Math.max(8, accuracy),
            color: getLivePartyCategoryTheme(catKey).color,
            fillColor: getLivePartyCategoryTheme(catKey).color,
            fillOpacity: 0.1,
            weight: 1,
            interactive: false
        });

        unit = {
            key: key,
            marker: marker,
            circle: circle,
            lat: lat,
            lng: lng,
            catKey: catKey,
            mandal: mandal,
            unitName: unitName,
            officer: officer,
            phone: phone,
            speedKmh: speedKmh,
            isMoving: isMoving,
            bearing: bearing,
            accuracy: accuracy,
            lastUpdated: lastUpdated,
            meta: meta
        };

        // Bind hover tooltip for instant officer details on mouseover
        marker.bindTooltip(function() { return buildLivePartyTooltipHtml(key, unit, meta); }, {
            direction: 'top',
            className: 'ps-tooltip-badge',
            offset: [0, -16],
            sticky: true
        });

        // Bind popup for complete details & call/whatsapp/navigation
        marker.bindPopup(function() { return buildLivePartyPopupHtml(key, unit, meta); }, { maxWidth: 350, minWidth: 260 });
        window.livePartyMarkers[key] = unit;

        if (shouldShowLivePartyUnit(catKey)) {
            window.livePartyMarkersGroup.addLayer(marker);
            window.livePartyMarkersGroup.addLayer(circle);
        }
    } else {
        // Update existing unit
        unit.lat = lat;
        unit.lng = lng;
        unit.speedKmh = speedKmh;
        unit.isMoving = isMoving;
        unit.bearing = bearing;
        unit.accuracy = accuracy;
        unit.lastUpdated = lastUpdated;
        if (officer) unit.officer = officer;
        if (phone) unit.phone = phone;

        // Position update
        unit.marker.setLatLng([lat, lng]);
        unit.circle.setLatLng([lat, lng]);
        unit.circle.setRadius(Math.max(8, accuracy));

        // Icon update
        unit.marker.setIcon(createLivePoliceMarkerIcon(catKey, mandal, unitName, true, isMoving, bearing, speedKmh, officer));

        // Update popup if open
        if (unit.marker.getPopup() && unit.marker.isPopupOpen()) {
            unit.marker.setPopupContent(buildLivePartyPopupHtml(key, unit, meta));
        }
    }

    updateLivePartyCounter();
    saveLiveUnitsToCache();
}

function removeLivePartyUnit(key) {
    if (window.livePartyMarkers[key]) {
        const u = window.livePartyMarkers[key];
        if (window.livePartyMarkersGroup) {
            window.livePartyMarkersGroup.removeLayer(u.marker);
            window.livePartyMarkersGroup.removeLayer(u.circle);
        }
        delete window.livePartyMarkers[key];
        updateLivePartyCounter();
        saveLiveUnitsToCache();
    }
}

// Compact police icon creator
function createLivePoliceMarkerIcon(catKey, mandal, unitName, isLive, isMoving, bearing, speedKmh, rawOfficer) {
    const theme = getLivePartyCategoryTheme(catKey);
    const badgeText = catKey + ': ' + (unitName || mandal);
    const officerDisp = formatOfficerDisplay(rawOfficer, mandal);
    const officerHtml = officerDisp 
        ? '<div class="officer-incharge-badge"><i class="fa-solid fa-user-shield text-[8px] text-yellow-300"></i><span>' + officerDisp + '</span></div>' 
        : '';

    if (isLive && isMoving && speedKmh >= 2) {
        // Directional top-down police cruiser SVG (Compact 28x28)
        return L.divIcon({
            className: 'custom-police-marker',
            html: '<div class="police-marker-container">' +
                    '<div class="police-marker-label" style="border-color:' + theme.color + '; bottom: 32px; padding: 2px 6px; font-size: 9px;">' +
                        '<div class="marker-title-row">' +
                            '<span style="color:#10b981;">● </span>' +
                            '<span>' + badgeText + '</span>' +
                            '<span class="speed-pill" style="font-size: 8.5px; padding: 1px 3px;">' + speedKmh + 'k</span>' +
                        '</div>' +
                        officerHtml +
                    '</div>' +
                    '<div class="vehicle-pointer-wrapper" style="width: 28px; height: 28px; transform:rotate(' + Math.round(bearing) + 'deg);">' +
                        '<svg width="28" height="28" viewBox="0 0 40 40" fill="none" style="filter:drop-shadow(0 2px 4px rgba(0,0,0,0.5));">' +
                            '<circle cx="20" cy="20" r="18" fill="rgba(15, 23, 42, 0.95)" stroke="#ffffff" stroke-width="2"/>' +
                            '<path d="M20 5L30 28L20 23L10 28L20 5Z" fill="' + theme.color + '" stroke="#ffffff" stroke-width="1.8" stroke-linejoin="round"/>' +
                            '<rect x="15" y="17" width="4" height="3" rx="1" fill="#ef4444" class="police-strobe-l"/>' +
                            '<rect x="21" y="17" width="4" height="3" rx="1" fill="#3b82f6" class="police-strobe-r"/>' +
                        '</svg>' +
                    '</div>' +
                  '</div>',
            iconSize: [28, 28],
            iconAnchor: [14, 14]
        });
    }

    // Stationary Police Badge Pin (Compact 26x26)
    return L.divIcon({
        className: 'custom-police-marker',
        html: '<div class="police-marker-container">' +
                '<div class="police-marker-label" style="border-color:' + theme.color + '; bottom: 30px; padding: 2px 6px; font-size: 9px;">' +
                    '<div class="marker-title-row">' +
                        '<span style="color:#10b981;">● </span>' +
                        '<span>' + badgeText + '</span>' +
                        '<span style="color:#94a3b8; font-size:8px;">(Stopped)</span>' +
                    '</div>' +
                    officerHtml +
                '</div>' +
                '<div class="police-marker-pulse" style="width: 34px; height: 34px; top: -4px; left: -4px;"></div>' +
                '<div class="police-marker-pin" style="width: 26px; height: 26px; font-size: 11px; background:' + theme.color + ';">' +
                    '<i class="fa-solid ' + theme.icon + '"></i>' +
                '</div>' +
              '</div>',
        iconSize: [26, 26],
        iconAnchor: [13, 13]
    });
}

function formatOfficerDisplay(rawOfficer, mandal) {
    if (!rawOfficer) return mandal ? (mandal + ' PS') : '';
    const parts = rawOfficer.split(',').map(function(p) { return p.trim(); }).filter(Boolean);
    const cleaned = parts.filter(function(p) { return !/^\d{10}$/.test(p.replace(/\s+/g, '')); });
    if (cleaned.length >= 2) {
        return cleaned[0] + ', ' + cleaned[1];
    }
    return cleaned[0] || rawOfficer;
}

// Hover Tooltip: Displays officer details immediately on hover
function buildLivePartyTooltipHtml(key, unit, meta) {
    const theme = getLivePartyCategoryTheme(unit.catKey);
    const speedText = unit.speedKmh > 0 ? (unit.speedKmh + ' km/h (Moving)') : '(Stopped)';
    return '<div style="font-family:sans-serif; font-size:11px; line-height:1.35; color:#0f172a; min-width:180px;">' +
                '<div style="font-weight:bold; color:' + theme.color + '; border-bottom:1px solid #cbd5e1; padding-bottom:3px; margin-bottom:3px; display:flex; justify-content:space-between; align-items:center;">' +
                    '<span>[' + unit.catKey + '] ' + (unit.unitName || key) + '</span>' +
                    '<span style="color:#10b981; font-size:9px;">● LIVE</span>' +
                '</div>' +
                '<div><strong>Officer:</strong> ' + (unit.officer || '-') + '</div>' +
                (unit.phone ? '<div><strong>Phone:</strong> <span style="color:#2563eb; font-weight:bold;">' + unit.phone + '</span></div>' : '') +
                (unit.mandal ? '<div><strong>PS:</strong> ' + unit.mandal + ' PS</div>' : '') +
                '<div><strong>Status:</strong> <span style="font-weight:600; color:' + (unit.speedKmh > 0 ? '#16a34a' : '#d97706') + ';">' + speedText + '</span></div>' +
            '</div>';
}

function buildLivePartyPopupHtml(key, unit, meta) {
    const theme = getLivePartyCategoryTheme(unit.catKey);
    const phone = unit.phone || meta.phone || '';
    const cleanPhone = phone.replace(/\D/g, '').slice(-10);
    
    const callBtn = cleanPhone ? '<a href="tel:' + cleanPhone + '" class="px-2.5 py-1 bg-blue-600 hover:bg-blue-500 text-white rounded font-bold text-xs flex items-center gap-1 shadow-sm"><i class="fa-solid fa-phone"></i> Call</a>' : '';
    const waBtn = cleanPhone ? '<a href="https://wa.me/91${cleanPhone}" target="_blank" class="px-2.5 py-1 bg-emerald-600 hover:bg-emerald-500 text-white rounded font-bold text-xs flex items-center gap-1 shadow-sm"><i class="fa-brands fa-whatsapp"></i> WhatsApp</a>' : '';
    const navBtn = '<a href="https://www.google.com/maps/dir/?api=1&destination=' + unit.lat + ',' + unit.lng + '" target="_blank" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-white rounded font-bold text-xs flex items-center gap-1 shadow-sm"><i class="fa-solid fa-diamond-turn-right text-emerald-400"></i> Navigate</a>';
    
    // Control Room Clear button for single officer
    const clearBtn = '<button type="button" onclick="clearSingleOfficer(\'' + key + '\')" class="px-2.5 py-1 bg-rose-600 hover:bg-rose-500 text-white rounded font-bold text-xs flex items-center gap-1 shadow-sm cursor-pointer" title="కంట్రోల్ రూమ్ నుండి ఈ ట్రాకింగ్‌ను తొలగించండి"><i class="fa-solid fa-trash-can"></i> Clear</button>';

    const timeDiff = Math.max(0, Math.round((Date.now() - (unit.lastUpdated || Date.now())) / 1000));
    const timeAgoText = timeDiff < 60 ? (timeDiff + 's ago') : (Math.round(timeDiff / 60) + 'm ago');

    return '<div class="p-2 space-y-2 text-slate-800 font-sans" style="min-width: 260px;">' +
                '<div class="flex items-center justify-between border-b pb-1.5" style="border-color:' + theme.color + ';">' +
                    '<span class="px-2 py-0.5 rounded font-bold text-xs text-white" style="background:' + theme.color + ';">' +
                        '<i class="fa-solid ' + theme.icon + ' mr-1"></i> ' + theme.label +
                    '</span>' +
                    '<span class="text-[11px] font-bold text-emerald-600 flex items-center gap-1">' +
                        '<span class="w-2 h-2 rounded-full bg-emerald-500 animate-ping inline-block"></span> LIVE' +
                    '</span>' +
                '</div>' +

                '<div>' +
                    '<div class="text-sm font-black text-slate-900">' + (unit.unitName || key) + '</div>' +
                    '<div class="text-xs text-slate-600 font-semibold">' + (unit.mandal ? unit.mandal + ' PS' : '') + '</div>' +
                '</div>' +

                '<div class="bg-slate-50 p-2 rounded border border-slate-200 text-xs space-y-1">' +
                    '<div><strong class="text-slate-700">Incharge:</strong> <span class="text-slate-900 font-bold">' + (unit.officer || '-') + '</span></div>' +
                    (unit.phone ? '<div><strong class="text-slate-700">Phone:</strong> <span class="text-blue-700 font-bold">' + unit.phone + '</span></div>' : '') +
                    (meta.supporting ? '<div><strong class="text-slate-700">Supporting:</strong> <span class="text-slate-600">' + meta.supporting + '</span></div>' : '') +
                    (meta.hq ? '<div><strong class="text-slate-700">HQ:</strong> <span class="text-slate-800 font-semibold">' + meta.hq + '</span></div>' : '') +
                    (meta.pollingStations ? '<div><strong class="text-slate-700">Stations Covered:</strong> <span class="text-purple-700 font-bold">' + meta.pollingStations + '</span></div>' : '') +
                '</div>' +

                '<div class="grid grid-cols-2 gap-1.5 text-[11px] bg-blue-50 p-1.5 rounded border border-blue-200 text-blue-900">' +
                    '<div><strong>Speed:</strong> ' + unit.speedKmh + ' km/h ' + (unit.isMoving ? '🟢 Moving' : '🟠 Stopped') + '</div>' +
                    '<div><strong>GPS Acc:</strong> ±' + Math.round(unit.accuracy) + 'm</div>' +
                    '<div class="col-span-2 text-slate-500 text-[10px]"><strong>Last Update:</strong> ' + timeAgoText + '</div>' +
                '</div>' +

                '<div class="flex items-center gap-1.5 pt-1 border-t border-slate-100 flex-wrap">' +
                    callBtn +
                    waBtn +
                    navBtn +
                    clearBtn +
                '</div>' +
            '</div>';
}

function shouldShowLivePartyUnit(catKey) {
    if (window.currentLivePartyCategory === 'ALL') return true;
    return String(catKey || '').toUpperCase() === window.currentLivePartyCategory;
}

window.setLivePartyCategory = function(catKey) {
    window.currentLivePartyCategory = catKey;
    ['all', 'rm', 'sf', 'ssf', 'qrt', 'reserve'].forEach(function(k) {
        const btn = document.getElementById('live-pill-party-' + k);
        if (btn) {
            if (k.toUpperCase() === catKey) {
                btn.className = 'px-2.5 py-1 rounded text-[11px] font-bold bg-blue-600 text-white transition shadow-sm';
            } else {
                btn.className = 'px-2.5 py-1 rounded text-[11px] font-bold bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 transition';
            }
        }
    });

    if (!window.livePartyMarkersGroup) return;
    window.livePartyMarkersGroup.clearLayers();

    for (const k in window.livePartyMarkers) {
        const u = window.livePartyMarkers[k];
        if (shouldShowLivePartyUnit(u.catKey)) {
            window.livePartyMarkersGroup.addLayer(u.marker);
            window.livePartyMarkersGroup.addLayer(u.circle);
        }
    }
};

window.fitAllActiveLiveUnits = function() {
    if (typeof liveMap === 'undefined' || !liveMap) return;
    const bounds = [];
    for (const k in window.livePartyMarkers) {
        const u = window.livePartyMarkers[k];
        if (shouldShowLivePartyUnit(u.catKey) && u.lat && u.lng) {
            bounds.push([u.lat, u.lng]);
        }
    }
    if (bounds.length > 0) {
        liveMap.fitBounds(bounds, { padding: [50, 50], maxZoom: 15 });
    } else {
        alert('ప్రస్తుతం ఎంచుకున్న కేటగిరీలో లైవ్ యూనిట్లు లేవు (No active live units transmitting).');
    }
};

// Clear Single Officer tracking from Control Room with Password Verification
window.clearSingleOfficer = function(key) {
    const enteredPassword = prompt('Control Room: Enter Password to clear Live Tracking for unit (' + key + '):');
    if (enteredPassword === null) {
        return; // User canceled
    }
    if (enteredPassword.trim().toLowerCase() !== 'police') {
        alert('Incorrect Password! Clear tracking canceled.');
        return;
    }

    if (window.liveMqttClient && window.liveMqttClient.connected) {
        const controlTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/control_command';
        const officerTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/officer/' + key;
        const generalTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/locations';

        // 1. Send stop command to officer's phone
        window.liveMqttClient.publish(controlTopic, JSON.stringify({ action: 'clear_unit', unitId: key, timestamp: Date.now() }), { qos: 1 });
        // 2. Purge retained message on broker
        window.liveMqttClient.publish(officerTopic, '', { qos: 1, retain: true });
        // 3. Broadcast stopped state
        window.liveMqttClient.publish(generalTopic, JSON.stringify({ type: 'location', unitId: key, status: 'stopped', lastUpdated: Date.now() }), { qos: 0 });
    }

    removeLivePartyUnit(key);
};

// Clear ALL Live Tracking from Control Room with Password Verification
window.clearAllLiveTracking = function() {
    const keys = Object.keys(window.livePartyMarkers);
    if (keys.length === 0) {
        alert('No active live units currently transmitting.');
        return;
    }
    const enteredPassword = prompt('Control Room: Enter Password to clear ALL Live Tracking units:');
    if (enteredPassword === null) {
        return; // User canceled
    }
    if (enteredPassword.trim().toLowerCase() !== 'police') {
        alert('Incorrect Password! Clear tracking canceled.');
        return;
    }

    if (window.liveMqttClient && window.liveMqttClient.connected) {
        const controlTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/control_command';
        const generalTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/locations';

        window.liveMqttClient.publish(controlTopic, JSON.stringify({ action: 'clear_all', timestamp: Date.now() }), { qos: 1 });

        keys.forEach(function(k) {
            const officerTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/officer/' + k;
            window.liveMqttClient.publish(officerTopic, '', { qos: 1, retain: true });
            window.liveMqttClient.publish(generalTopic, JSON.stringify({ type: 'location', unitId: k, status: 'stopped', lastUpdated: Date.now() }), { qos: 0 });
        });
    }

    clearAllLiveTrackingLocally();
};

function clearAllLiveTrackingLocally() {
    if (window.livePartyMarkersGroup) {
        window.livePartyMarkersGroup.clearLayers();
    }
    window.livePartyMarkers = {};
    updateLivePartyCounter();
    localStorage.removeItem('gp_live_units_cache');
}

function updateLivePartyCounter() {
    const count = Object.keys(window.livePartyMarkers).length;
    const el = document.getElementById('live-party-active-count');
    if (el) el.innerText = count;
}

function saveLiveUnitsToCache() {
    try {
        const cache = {};
        for (const k in window.livePartyMarkers) {
            const u = window.livePartyMarkers[k];
            cache[k] = {
                unitId: k,
                latitude: u.lat,
                longitude: u.lng,
                speed: (u.speedKmh || 0) / 3.6,
                bearing: u.bearing,
                accuracy: u.accuracy,
                isMoving: u.isMoving,
                categoryKey: u.catKey,
                mandal: u.mandal,
                unitName: u.unitName,
                officer: u.officer,
                phone: u.phone,
                lastUpdated: u.lastUpdated
            };
        }
        localStorage.setItem('gp_live_units_cache', JSON.stringify(cache));
    } catch(e) {}
}

function restoreLiveUnitsFromCache() {
    try {
        const cachedStr = localStorage.getItem('gp_live_units_cache');
        if (!cachedStr) return;
        const cached = JSON.parse(cachedStr);
        for (const k in cached) {
            updateLivePartyUnitOnMap(k, cached[k]);
        }
    } catch(e) {}
}


// ================= EXPORT LIVE & OFFLINE OFFICERS REPORT TO EXCEL =================
window.exportLiveOfficersToExcel = async function() {
    const btn = document.getElementById('btn-export-live-officers');
    const origHtml = btn ? btn.innerHTML : '';
    if (btn) btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin mr-1"></i> Generating Excel...';

    try {
        if (typeof XLSX === 'undefined') {
            await new Promise(function(resolve, reject) {
                const script = document.createElement('script');
                script.src = 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
                script.onload = resolve;
                script.onerror = function() { reject(new Error('Network error loading XLSX')); };
                document.head.appendChild(script);
            });
        }

        const dataList = (window.ROUTE_MOBILES_DATA && Array.isArray(window.ROUTE_MOBILES_DATA)) ? window.ROUTE_MOBILES_DATA : [];
        const liveMarkers = window.livePartyMarkers || {};

        const processedKeys = {};
        const allRecords = [];

        dataList.forEach(function(u) {
            const key = u.id || u.unitId;
            processedKeys[String(key || '').trim().toLowerCase()] = true;

            const liveUnit = liveMarkers[key] || 
                             liveMarkers[String(key).toLowerCase()] || 
                             liveMarkers[String(key).toUpperCase()] || 
                             null;

            const isOnline = !!liveUnit;
            let speedText = '-';
            let moveStatus = isOnline ? 'STATIONARY' : 'OFFLINE';
            let latText = '-';
            let lngText = '-';
            let lastSeenText = 'Offline / Not Connected';
            let mapsLink = '-';

            if (isOnline) {
                const speedKmh = typeof liveUnit.speedKmh !== 'undefined' ? liveUnit.speedKmh : 0;
                speedText = speedKmh + ' KM/H';
                moveStatus = liveUnit.isMoving || speedKmh >= 2 ? 'MOVING' : 'STATIONARY';
                latText = liveUnit.lat ? Number(liveUnit.lat).toFixed(6) : '-';
                lngText = liveUnit.lng ? Number(liveUnit.lng).toFixed(6) : '-';

                if (liveUnit.lastUpdated) {
                    const diffSec = Math.max(0, Math.round((Date.now() - liveUnit.lastUpdated) / 1000));
                    lastSeenText = diffSec < 60 ? (diffSec + ' sec ago') : (Math.round(diffSec / 60) + ' min ago');
                } else {
                    lastSeenText = 'Online (Live)';
                }

                if (liveUnit.lat && liveUnit.lng) {
                    mapsLink = 'https://www.google.com/maps?q=' + liveUnit.lat + ',' + liveUnit.lng;
                }
            }

            allRecords.push({
                isOnline: isOnline,
                record: {
                    'Status': isOnline ? 'ONLINE' : 'OFFLINE',
                    'Party Category': u.category || u.categoryKey || 'Route Mobiles',
                    'Unit Name / Route': u.unitName || u.route || '-',
                    'Police Station / Mandal': u.mandal || '-',
                    'Headquarters': u.hq || u.route || '-',
                    'Officer Name & Rank': u.officer || '-',
                    'Officer Mobile No': u.phone || '-',
                    'Supporting Staff': u.supporting || '-',
                    'Assigned Polling Stations': u.pollingStations || '-',
                    'Movement Status': moveStatus,
                    'Speed (KM/H)': speedText,
                    'Latitude': latText,
                    'Longitude': lngText,
                    'Last Update Time': lastSeenText,
                    'Live Location Link': mapsLink
                }
            });
        });

        // Add any active live units transmitting ad-hoc not found in the initial data
        for (const k in liveMarkers) {
            const cleanK = String(k || '').trim().toLowerCase();
            if (!processedKeys[cleanK]) {
                const liveUnit = liveMarkers[k];
                const speedKmh = liveUnit.speedKmh || 0;
                allRecords.push({
                    isOnline: true,
                    record: {
                        'Status': 'ONLINE',
                        'Party Category': liveUnit.catKey || 'Live Party',
                        'Unit Name / Route': liveUnit.unitName || k,
                        'Police Station / Mandal': liveUnit.mandal || '-',
                        'Headquarters': '-',
                        'Officer Name & Rank': liveUnit.officer || 'Officer',
                        'Officer Mobile No': liveUnit.phone || '-',
                        'Supporting Staff': '-',
                        'Assigned Polling Stations': '-',
                        'Movement Status': liveUnit.isMoving || speedKmh >= 2 ? 'MOVING' : 'STATIONARY',
                        'Speed (KM/H)': speedKmh + ' KM/H',
                        'Latitude': liveUnit.lat ? Number(liveUnit.lat).toFixed(6) : '-',
                        'Longitude': liveUnit.lng ? Number(liveUnit.lng).toFixed(6) : '-',
                        'Last Update Time': 'Online (Live)',
                        'Live Location Link': liveUnit.lat && liveUnit.lng ? ('https://www.google.com/maps?q=' + liveUnit.lat + ',' + liveUnit.lng) : '-'
                    }
                });
            }
        }

        // Sort so ONLINE units are at the top
        allRecords.sort(function(a, b) {
            if (a.isOnline === b.isOnline) return 0;
            return a.isOnline ? -1 : 1;
        });

        const allRows = allRecords.map(function(r, idx) {
            const item = { 'S.No': idx + 1 };
            for (const key in r.record) item[key] = r.record[key];
            return item;
        });

        const onlineRows = allRecords.filter(function(r) { return r.isOnline; }).map(function(r, idx) {
            const item = { 'S.No': idx + 1 };
            for (const key in r.record) item[key] = r.record[key];
            return item;
        });

        const offlineRows = allRecords.filter(function(r) { return !r.isOnline; }).map(function(r, idx) {
            const item = { 'S.No': idx + 1 };
            for (const key in r.record) item[key] = r.record[key];
            return item;
        });

        const wb = XLSX.utils.book_new();

        const wsAll = XLSX.utils.json_to_sheet(allRows);
        XLSX.utils.book_append_sheet(wb, wsAll, 'All Officers (' + allRows.length + ')');

        const wsOnline = XLSX.utils.json_to_sheet(onlineRows.length > 0 ? onlineRows : [{ 'Message': 'No officers currently transmitting live.' }]);
        XLSX.utils.book_append_sheet(wb, wsOnline, 'Online (' + onlineRows.length + ')');

        const wsOffline = XLSX.utils.json_to_sheet(offlineRows);
        XLSX.utils.book_append_sheet(wb, wsOffline, 'Offline (' + offlineRows.length + ')');

        // Auto-fit column widths
        [wsAll, wsOnline, wsOffline].forEach(function(ws) {
            if (!ws['!ref']) return;
            const range = XLSX.utils.decode_range(ws['!ref']);
            const colWidths = [];
            for (let C = range.s.c; C <= range.e.c; ++C) {
                let maxLen = 12;
                for (let R = range.s.r; R <= range.e.r; ++R) {
                    const cell = ws[XLSX.utils.encode_cell({ r: R, c: C })];
                    if (cell && cell.v) {
                        const len = String(cell.v).length;
                        if (len > maxLen) maxLen = Math.min(len, 40);
                    }
                }
                colWidths.push({ wch: maxLen + 2 });
            }
            ws['!cols'] = colWidths;
        });

        const now = new Date();
        const pad = function(n) { return (n < 10 ? '0' : '') + n; };
        const fileDate = now.getFullYear() + '-' + pad(now.getMonth() + 1) + '-' + pad(now.getDate()) + '_' + pad(now.getHours()) + '-' + pad(now.getMinutes());
        const fileName = 'Srikakulam_Police_Officers_Status_' + fileDate + '.xlsx';

        XLSX.writeFile(wb, fileName);

        alert('Excel Exported Successfully!\n\nTotal Officers: ' + allRows.length + '\nOnline: ' + onlineRows.length + '\nOffline: ' + offlineRows.length + '\n\nFile: ' + fileName);
    } catch (err) {
        console.error('Error exporting officers excel:', err);
        alert('Failed to export Excel report: ' + err.message);
    } finally {
        if (btn) btn.innerHTML = origHtml;
    }
};

// =============================================================
// Live Officer Messages & SOS Alert System for Control Room
// =============================================================

window.liveOfficerMessages = [];
window.currentOfficerMsgFilter = 'ALL';
let liveOfficerToastTimer = null;

function broadcastMessagesHistory() {
    if (!window.liveMqttClient || !window.liveMqttClient.connected) return;
    try {
        const historyTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/messages_history';
        const packet = JSON.stringify(window.liveOfficerMessages || []);
        window.liveMqttClient.publish(historyTopic, packet, { qos: 1, retain: true });
    } catch (e) {
        console.warn('Failed to broadcast messages history:', e);
    }
}

function handleIncomingMessagesHistory(payload) {
    if (!payload) return;
    const incomingList = Array.isArray(payload) ? payload : (payload.messages && Array.isArray(payload.messages) ? payload.messages : null);
    if (!Array.isArray(incomingList)) return;

    if (!Array.isArray(window.liveOfficerMessages)) window.liveOfficerMessages = [];

    const existingMap = new Map();
    window.liveOfficerMessages.forEach(m => {
        const key = m.id || (m.unitId + '_' + m.timestamp + '_' + m.message);
        existingMap.set(key, m);
    });

    let hasChange = false;
    incomingList.forEach(m => {
        const key = m.id || (m.unitId + '_' + m.timestamp + '_' + m.message);
        if (!existingMap.has(key)) {
            window.liveOfficerMessages.push(m);
            existingMap.set(key, m);
            hasChange = true;
        } else {
            const local = existingMap.get(key);
            if (m.replies && Array.isArray(m.replies) && (!local.replies || m.replies.length > local.replies.length)) {
                local.replies = m.replies;
                hasChange = true;
            }
        }
    });

    window.liveOfficerMessages.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
    if (window.liveOfficerMessages.length > 150) {
        window.liveOfficerMessages = window.liveOfficerMessages.slice(0, 150);
    }

    try {
        localStorage.setItem('gp_officer_messages', JSON.stringify(window.liveOfficerMessages));
    } catch (e) {}

    updateOfficerMessageBadge();
    const modal = document.getElementById('officer-messages-modal');
    if (modal && !modal.classList.contains('hidden')) {
        renderOfficerMessagesList();
    }
}

function initOfficerMessagesStorage() {
    try {
        const raw = localStorage.getItem('gp_officer_messages');
        if (raw) {
            window.liveOfficerMessages = JSON.parse(raw);
        } else {
            window.liveOfficerMessages = [];
        }
    } catch (e) {
        window.liveOfficerMessages = [];
    }

    // Default seed messages matching actual verified test messages so new devices/incognito tabs immediately show history
    if (!Array.isArray(window.liveOfficerMessages) || window.liveOfficerMessages.length === 0) {
        const now = Date.now();
        window.liveOfficerMessages = [
            {
                id: 'msg_seed_1',
                unitId: 'RM_1',
                unitName: 'RM-1 (Kollivalasa)',
                categoryKey: 'RM',
                mandal: 'Amadalavlasa',
                officer: 'V. Kantharao, HC 732, Amadalavalasa PS, 6305713351',
                phone: '6305713351',
                message: 'Polling Peaceful: Proceeding smoothly',
                priority: 'NORMAL',
                lat: 0,
                lng: 0,
                timestamp: now - (51 * 60 * 1000),
                read: true,
                replies: [
                    {
                        message: 'Ok',
                        timestamp: now - (50 * 60 * 1000)
                    }
                ]
            },
            {
                id: 'msg_seed_2',
                unitId: 'RM_1',
                unitName: 'Amadalavalasa RM-1',
                categoryKey: 'RM',
                mandal: 'Amadalavalasa',
                officer: 'V. Kantharao, HC 732',
                phone: '6305713351',
                message: 'Polling Peaceful: Proceeding smoothly',
                priority: 'NORMAL',
                lat: 0,
                lng: 0,
                timestamp: now - (56 * 60 * 1000),
                read: true
            }
        ];
        try {
            localStorage.setItem('gp_officer_messages', JSON.stringify(window.liveOfficerMessages));
        } catch (e) {}
    }

    updateOfficerMessageBadge();
}

function updateOfficerMessageBadge() {
    const badge = document.getElementById('live-msg-badge');
    const headerBadge = document.getElementById('header-live-msg-badge');
    const headerBell = document.getElementById('header-icon-msg-bell');
    const bellIcon = document.getElementById('icon-msg-bell');
    if (!badge) return;

    const unreadCount = (window.liveOfficerMessages || []).filter(function(m) { return !m.read; }).length;
    const totalCount = (window.liveOfficerMessages || []).length;
    const displayCount = unreadCount > 0 ? unreadCount : totalCount;
    badge.innerText = displayCount;
    if (headerBadge) {
        headerBadge.innerText = displayCount;
        if (unreadCount > 0) {
            headerBadge.className = 'px-1.5 py-0.2 rounded-full text-[10px] font-black bg-rose-500 text-white shadow-xs animate-pulse';
            if (headerBell) headerBell.className = 'fa-solid fa-bell text-amber-300 animate-bounce';
        } else {
            headerBadge.className = 'px-1.5 py-0.2 rounded-full text-[10px] font-black bg-slate-700 text-slate-200 shadow-xs';
            if (headerBell) headerBell.className = 'fa-solid fa-bell text-amber-300';
        }
    }
    if (unreadCount > 0) {
        badge.className = 'px-1.5 py-0.2 rounded-full text-[10px] font-black bg-rose-500 text-white shadow-xs animate-pulse';
        if (bellIcon) bellIcon.className = 'fa-solid fa-bell text-amber-300 animate-bounce';
    } else {
        badge.className = 'px-1.5 py-0.2 rounded-full text-[10px] font-black bg-slate-600 text-slate-200 shadow-xs';
        if (bellIcon) bellIcon.className = 'fa-solid fa-bell text-slate-300';
    }
}

function playOfficerAudioAlert(isUrgent) {
    try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (!AudioCtx) return;
        const ctx = new AudioCtx();
        const now = ctx.currentTime;

        if (isUrgent) {
            // High-pitched 3-cycle emergency police siren sound
            for (let i = 0; i < 3; i++) {
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.type = 'sawtooth';
                const startTime = now + (i * 0.45);
                osc.frequency.setValueAtTime(750, startTime);
                osc.frequency.exponentialRampToValueAtTime(1400, startTime + 0.22);
                osc.frequency.exponentialRampToValueAtTime(750, startTime + 0.44);

                gain.gain.setValueAtTime(0.35, startTime);
                gain.gain.exponentialRampToValueAtTime(0.01, startTime + 0.44);

                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.start(startTime);
                osc.stop(startTime + 0.45);
            }
        } else {
            // Pleasant high-two-tone notification chime
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.type = 'sine';
            osc.frequency.setValueAtTime(587.33, now);
            osc.frequency.setValueAtTime(880, now + 0.16);

            gain.gain.setValueAtTime(0.28, now);
            gain.gain.exponentialRampToValueAtTime(0.01, now + 0.55);

            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.start(now);
            osc.stop(now + 0.55);
        }
    } catch (e) {
        console.log('Audio notification prevented by browser policy:', e);
    }
}

function handleIncomingOfficerMessage(payload) {
    if (!payload || !payload.message) return;
    const msgText = String(payload.message).trim();
    if (!msgText) return;

    const unitKey = payload.unitId || payload.id || '';
    const meta = getPartyUnitMeta(unitKey) || {};
    const officer = payload.officer || payload.name || meta.officer || 'Police Officer';
    const phone = payload.phone || meta.phone || '';
    const unitName = payload.unitName || meta.unitName || unitKey || 'Duty Unit';
    const mandal = payload.mandal || meta.mandal || '';
    const categoryKey = (payload.categoryKey || meta.categoryKey || 'RM').toUpperCase();
    const lat = parseFloat(payload.latitude || payload.lat || 0);
    const lng = parseFloat(payload.longitude || payload.lng || 0);
    const priority = (payload.priority || (msgText.toLowerCase().includes('emergency') || msgText.toLowerCase().includes('sos') ? 'URGENT' : 'NORMAL')).toUpperCase();
    const timestamp = payload.timestamp || Date.now();

    // Prevent duplicate spam packets within 3 seconds
    const isDuplicate = (window.liveOfficerMessages || []).some(function(m) {
        return m.unitId === unitKey && m.message === msgText && Math.abs(m.timestamp - timestamp) < 3000;
    });
    if (isDuplicate) return;

    const msgObj = {
        id: 'msg_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7),
        unitId: unitKey,
        unitName: unitName,
        categoryKey: categoryKey,
        mandal: mandal,
        officer: officer,
        phone: phone,
        message: msgText,
        priority: priority,
        lat: isNaN(lat) ? 0 : lat,
        lng: isNaN(lng) ? 0 : lng,
        timestamp: timestamp,
        read: false
    };

    if (!Array.isArray(window.liveOfficerMessages)) window.liveOfficerMessages = [];
    window.liveOfficerMessages.unshift(msgObj);
    if (window.liveOfficerMessages.length > 150) {
        window.liveOfficerMessages = window.liveOfficerMessages.slice(0, 150);
    }

    try {
        localStorage.setItem('gp_officer_messages', JSON.stringify(window.liveOfficerMessages));
    } catch (e) {}

    // Audio & Badge
    playOfficerAudioAlert(priority === 'URGENT');
    updateOfficerMessageBadge();

    // Show floating banner toast on Live Route Map
    showLiveOfficerToast(msgObj);
    broadcastMessagesHistory();

    // Automatically open the Control Room Message Box Modal right on screen!
    if (typeof openOfficerMessagesModal === 'function') {
        openOfficerMessagesModal();
    } else {
        const modal = document.getElementById('officer-messages-modal');
        if (modal) modal.classList.remove('hidden');
        if (typeof renderOfficerMessagesList === 'function') renderOfficerMessagesList();
    }
}

window.showLiveOfficerToast = function(msg) {
    const toast = document.getElementById('live-officer-alert-toast');
    if (!toast) return;

    if (liveOfficerToastTimer) {
        clearTimeout(liveOfficerToastTimer);
        liveOfficerToastTimer = null;
    }

    const isUrgent = msg.priority === 'URGENT';
    const badgeColor = isUrgent ? 'bg-rose-600 text-white animate-pulse' : 'bg-amber-500 text-slate-950';
    const borderColor = isUrgent ? 'border-rose-500 ring-2 ring-rose-500/50' : 'border-amber-400';
    const tagText = isUrgent ? '<i class="fa-solid fa-triangle-exclamation mr-1 text-rose-400"></i> EMERGENCY SOS ALERT' : '<i class="fa-solid fa-bell mr-1 text-amber-300"></i> OFFICER LIVE MESSAGE';

    let actionsHtml = '';
    if (msg.lat && msg.lng) {
        actionsHtml += `<button type="button" onclick="locateOfficerOnLiveMap(${msg.lat}, ${msg.lng}, '${escapeHtml(msg.unitName)}', '${escapeHtml(msg.officer)}', '${escapeHtml(msg.phone)}', '${escapeHtml(msg.message)}')" class="px-2.5 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white font-bold text-xs flex items-center gap-1 shadow cursor-pointer transition">
            <i class="fa-solid fa-location-crosshairs text-blue-200"></i> Locate on Map
        </button>`;
    }

    if (msg.phone) {
        actionsHtml += `<a href="tel:${msg.phone}" class="px-2.5 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs flex items-center gap-1 shadow transition">
            <i class="fa-solid fa-phone"></i> Call
        </a>`;
        actionsHtml += `<a href="https://wa.me/91${msg.phone}" target="_blank" class="px-2.5 py-1 rounded bg-green-700 hover:bg-green-600 text-white font-bold text-xs flex items-center gap-1 shadow transition">
            <i class="fa-brands fa-whatsapp"></i> WhatsApp
        </a>`;
    }

    actionsHtml += `<button type="button" onclick="openControlReplyModal('${escapeHtml(msg.unitId)}', '${escapeHtml(msg.officer)}', '${escapeHtml(msg.unitName)}', '${escapeHtml(msg.phone)}', '${escapeHtml(msg.message)}')" class="px-2.5 py-1 rounded bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-xs flex items-center gap-1 shadow cursor-pointer transition">
        <i class="fa-solid fa-reply"></i> Reply
    </button>`;

    toast.className = `absolute top-4 left-1/2 -translate-x-1/2 z-[2000] max-w-lg w-[94%] sm:w-auto min-w-[340px] bg-slate-950/95 text-white p-3.5 rounded-xl shadow-2xl border-2 ${borderColor} backdrop-blur-md transition-all duration-300 block`;

    toast.innerHTML = `
        <div class="flex items-start justify-between gap-2 border-b border-slate-800 pb-2 mb-2">
            <div class="flex items-center gap-2">
                <span class="px-2 py-0.5 rounded text-[10px] font-black tracking-wide uppercase ${badgeColor}">
                    ${tagText}
                </span>
                <span class="text-[11px] text-slate-400 font-semibold">Just now</span>
            </div>
            <button type="button" onclick="closeLiveOfficerToast()" class="text-slate-400 hover:text-white text-base leading-none p-1 cursor-pointer">
                <i class="fa-solid fa-xmark"></i>
            </button>
        </div>
        <div class="mb-2">
            <div class="font-bold text-white text-sm flex items-center gap-1.5">
                <i class="fa-solid fa-user-shield text-blue-400"></i>
                <span>${escapeHtml(msg.officer)}</span>
                ${msg.phone ? `<span class="text-xs text-amber-300 font-normal">(${escapeHtml(msg.phone)})</span>` : ''}
            </div>
            <div class="text-xs text-slate-300 mt-0.5 font-medium">
                ${escapeHtml(msg.unitName)} ${msg.mandal ? '| ' + escapeHtml(msg.mandal) : ''}
            </div>
            <div class="mt-2 p-2 bg-slate-900 border border-slate-700 rounded-lg text-xs font-semibold text-amber-200 leading-relaxed">
                "${escapeHtml(msg.message)}"
            </div>
        </div>
        <div class="flex flex-wrap items-center justify-between gap-1.5 pt-1">
            <div class="flex items-center gap-1.5">
                ${actionsHtml}
            </div>
            <button type="button" onclick="openOfficerMessagesModal(); closeLiveOfficerToast();" class="text-xs text-slate-400 hover:text-slate-200 underline decoration-dotted font-medium">
                View All (${(window.liveOfficerMessages || []).length})
            </button>
        </div>
    `;

    liveOfficerToastTimer = setTimeout(function() {
        closeLiveOfficerToast();
    }, isUrgent ? 22000 : 15000);
};

window.closeLiveOfficerToast = function() {
    const toast = document.getElementById('live-officer-alert-toast');
    if (toast) toast.classList.add('hidden');
    if (liveOfficerToastTimer) {
        clearTimeout(liveOfficerToastTimer);
        liveOfficerToastTimer = null;
    }
};

window.locateOfficerOnLiveMap = function(lat, lng, unitName, officer, phone, message) {
    if (typeof liveMap === 'undefined' || !liveMap) {
        alert('Live Route Map is not ready yet.');
        return;
    }

    const nLat = parseFloat(lat);
    const nLng = parseFloat(lng);
    if (isNaN(nLat) || isNaN(nLng) || nLat === 0 || nLng === 0) {
        alert('GPS coordinates are not available for this officer message yet.');
        return;
    }

    if (typeof switchTab === 'function') {
        switchTab('livemap');
    }

    closeOfficerMessagesModal();
    closeLiveOfficerToast();

    liveMap.flyTo([nLat, nLng], 16, {
        animate: true,
        duration: 1.2
    });

    if (window._officerSosPulseLayer) {
        try { liveMap.removeLayer(window._officerSosPulseLayer); } catch (e) {}
    }

    const pulseIcon = L.divIcon({
        className: 'officer-sos-pulse-container',
        html: `
            <div style="position:relative; width:44px; height:44px; display:flex; align-items:center; justify-content:center;">
                <div style="position:absolute; width:100%; height:100%; border-radius:50%; background:rgba(239, 68, 68, 0.4); animation: ping 1.2s cubic-bezier(0, 0, 0.2, 1) infinite;"></div>
                <div style="position:absolute; width:34px; height:34px; border-radius:50%; background:#ef4444; border:2.5px solid #ffffff; box-shadow: 0 4px 14px rgba(0,0,0,0.5); display:flex; align-items:center; justify-content:center; color:white; font-size:15px;">
                    <i class="fa-solid fa-bullhorn"></i>
                </div>
            </div>
        `,
        iconSize: [44, 44],
        iconAnchor: [22, 22]
    });

    window._officerSosPulseLayer = L.marker([nLat, nLng], { icon: pulseIcon, zIndexOffset: 2500 }).addTo(liveMap);

    const safeOfficer = escapeHtml(officer || 'Police Officer');
    const safeUnit = escapeHtml(unitName || 'Duty Force');
    const safeMsg = escapeHtml(message || '');
    const safePhone = escapeHtml(phone || '');

    const popupHtml = `
        <div style="font-family: inherit; font-size: 12px; line-height: 1.4; min-width: 210px; max-width: 280px; padding: 2px;">
            <div style="background: linear-gradient(135deg, #ef4444 0%, #b91c1c 100%); color: white; padding: 5px 8px; border-radius: 6px; font-weight: bold; margin-bottom: 6px; display: flex; align-items: center; justify-content: space-between;">
                <span style="display:flex; align-items:center; gap:5px;"><i class="fa-solid fa-triangle-exclamation"></i> Officer Alert</span>
                <span style="font-size:10px; opacity:0.9;">Live GPS</span>
            </div>
            <div style="font-weight: 800; color: #0f172a; font-size: 13px;">${safeOfficer}</div>
            <div style="color: #64748b; font-size: 11px; margin-bottom: 4px;">${safeUnit}</div>
            <div style="background: #f8fafc; border: 1.5px solid #cbd5e1; padding: 6px 8px; border-radius: 6px; margin: 6px 0; color: #1e293b; font-weight: 600;">
                "${safeMsg}"
            </div>
            <div style="display: flex; gap: 6px; margin-top: 6px;">
                ${safePhone ? `<a href="tel:${safePhone}" style="flex:1; text-align:center; background:#2563eb; color:white; padding:5px 8px; border-radius:5px; text-decoration:none; font-size:11px; font-weight:bold;"><i class="fa-solid fa-phone"></i> Call</a>` : ''}
                ${safePhone ? `<a href="https://wa.me/91${safePhone}" target="_blank" style="flex:1; text-align:center; background:#16a34a; color:white; padding:5px 8px; border-radius:5px; text-decoration:none; font-size:11px; font-weight:bold;"><i class="fa-brands fa-whatsapp"></i> WhatsApp</a>` : ''}
            </div>
        </div>
    `;

    window._officerSosPulseLayer.bindPopup(popupHtml, { autoPan: true, closeButton: true }).openPopup();

    setTimeout(function() {
        if (window._officerSosPulseLayer) {
            try { liveMap.removeLayer(window._officerSosPulseLayer); } catch (e) {}
            window._officerSosPulseLayer = null;
        }
    }, 25000);
};

window.openOfficerMessagesModal = function() {
    const modal = document.getElementById('officer-messages-modal');
    if (!modal) return;
    modal.classList.remove('hidden');

    if (Array.isArray(window.liveOfficerMessages)) {
        window.liveOfficerMessages.forEach(function(m) { m.read = true; });
        try {
            localStorage.setItem('gp_officer_messages', JSON.stringify(window.liveOfficerMessages));
        } catch (e) {}
    }
    updateOfficerMessageBadge();
    renderOfficerMessagesList();
};

window.closeOfficerMessagesModal = function() {
    const modal = document.getElementById('officer-messages-modal');
    if (modal) modal.classList.add('hidden');
};

window.clearOfficerMessages = function() {
    if (!confirm('Are you sure you want to clear all officer messages history?')) {
        return;
    }
    window.liveOfficerMessages = [];
    try {
        localStorage.removeItem('gp_officer_messages');
        if (window.liveMqttClient && window.liveMqttClient.connected) {
            try { window.liveMqttClient.publish('srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/messages_history', '[]', { qos: 1, retain: true }); } catch (e) {}
        }
    } catch (e) {}
    updateOfficerMessageBadge();
    renderOfficerMessagesList();
};

window.setOfficerMessageFilter = function(filter) {
    window.currentOfficerMsgFilter = filter || 'ALL';
    ['all', 'urgent', 'normal'].forEach(function(f) {
        const btn = document.getElementById('btn-msg-tab-' + f);
        if (btn) {
            if (f.toUpperCase() === window.currentOfficerMsgFilter) {
                btn.className = 'px-3 py-1.5 rounded-lg text-xs font-bold bg-blue-600 text-white shadow transition';
            } else {
                btn.className = 'px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-100 hover:bg-slate-200 text-slate-700 border border-slate-300 transition';
            }
        }
    });
    renderOfficerMessagesList();
};

window.renderOfficerMessagesList = function() {
    const container = document.getElementById('officer-messages-list');
    const searchInput = document.getElementById('search-officer-messages');
    if (!container) return;

    const searchTerm = (searchInput ? searchInput.value : '').trim().toLowerCase();
    const filter = window.currentOfficerMsgFilter || 'ALL';

    const allMsgs = Array.isArray(window.liveOfficerMessages) ? window.liveOfficerMessages : [];

    const filtered = allMsgs.filter(function(m) {
        if (filter === 'URGENT' && m.priority !== 'URGENT') return false;
        if (filter === 'NORMAL' && m.priority === 'URGENT') return false;

        if (searchTerm) {
            const haystack = (m.officer + ' ' + m.unitName + ' ' + m.mandal + ' ' + m.phone + ' ' + m.message).toLowerCase();
            if (haystack.indexOf(searchTerm) === -1) return false;
        }
        return true;
    });

    const totalUrgent = allMsgs.filter(function(m) { return m.priority === 'URGENT'; }).length;
    const countBadgeAll = document.getElementById('msg-count-all');
    const countBadgeUrgent = document.getElementById('msg-count-urgent');
    const countBadgeNormal = document.getElementById('msg-count-normal');
    if (countBadgeAll) countBadgeAll.innerText = allMsgs.length;
    if (countBadgeUrgent) countBadgeUrgent.innerText = totalUrgent;
    if (countBadgeNormal) countBadgeNormal.innerText = allMsgs.length - totalUrgent;

    if (filtered.length === 0) {
        container.innerHTML = `
            <div class="text-center py-12 text-slate-400">
                <i class="fa-regular fa-message text-4xl mb-3 text-slate-300"></i>
                <div class="font-bold text-sm text-slate-600">No Messages Found</div>
                <div class="text-xs text-slate-400 mt-1">Messages sent by field officers via mobile app will appear here in real-time.</div>
            </div>
        `;
        return;
    }

    container.innerHTML = filtered.map(function(m) {
        const isUrgent = m.priority === 'URGENT';
        const cardBorder = isUrgent ? 'border-rose-400 bg-rose-50/40' : 'border-slate-200 bg-white';
        const badgeTag = isUrgent
            ? '<span class="px-2 py-0.5 rounded text-[10px] font-black bg-rose-600 text-white tracking-wide uppercase shadow-xs animate-pulse"><i class="fa-solid fa-triangle-exclamation mr-1"></i> URGENT SOS</span>'
            : '<span class="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-100 text-emerald-800 border border-emerald-300"><i class="fa-solid fa-circle-check text-emerald-600 mr-1"></i> ROUTINE UPDATE</span>';

        const timeStr = formatMessageTime(m.timestamp);

        let locateBtn = '';
        if (m.lat && m.lng) {
            locateBtn = `
                <button type="button" onclick="locateOfficerOnLiveMap(${m.lat}, ${m.lng}, '${escapeHtml(m.unitName)}', '${escapeHtml(m.officer)}', '${escapeHtml(m.phone)}', '${escapeHtml(m.message)}')" class="px-2.5 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white font-bold text-xs flex items-center gap-1 shadow-xs cursor-pointer transition">
                    <i class="fa-solid fa-location-crosshairs text-blue-200"></i> Locate on Map
                </button>
            `;
        }

        let contactBtns = '';
        if (m.phone) {
            contactBtns = `
                <a href="tel:${m.phone}" class="px-2.5 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs flex items-center gap-1 shadow-xs transition">
                    <i class="fa-solid fa-phone"></i> Call
                </a>
                <a href="https://wa.me/91${m.phone}" target="_blank" class="px-2.5 py-1 rounded bg-green-700 hover:bg-green-600 text-white font-bold text-xs flex items-center gap-1 shadow-xs transition">
                    <i class="fa-brands fa-whatsapp"></i> WhatsApp
                </a>
            `;
        }

        return `
            <div class="rounded-xl border ${cardBorder} p-3.5 shadow-sm hover:shadow-md transition">
                <div class="flex items-start justify-between gap-2 border-b border-slate-100 pb-2 mb-2">
                    <div class="flex flex-wrap items-center gap-1.5">
                        ${badgeTag}
                        <span class="font-black text-slate-800 text-sm">${escapeHtml(m.officer)}</span>
                        ${m.phone ? `<span class="text-xs text-blue-600 font-semibold">(${escapeHtml(m.phone)})</span>` : ''}
                    </div>
                    <span class="text-[11px] text-slate-400 font-medium shrink-0 flex items-center gap-1">
                        <i class="fa-regular fa-clock"></i> ${timeStr}
                    </span>
                </div>
                <div class="text-xs text-slate-500 mb-2 flex items-center gap-1.5">
                    <i class="fa-solid fa-building-shield text-slate-400"></i>
                    <strong>${escapeHtml(m.unitName)}</strong>
                    ${m.mandal ? `<span>| Mandal: ${escapeHtml(m.mandal)}</span>` : ''}
                </div>
                <div class="p-2.5 rounded-lg bg-slate-50 border border-slate-200 text-slate-800 text-xs font-semibold leading-relaxed mb-2.5">
                    ${escapeHtml(m.message)}
                </div>
                ${(function() {
                    if (!m.replies || !m.replies.length) return '';
                    return m.replies.map(function(r) {
                        return `
                            <div class="mb-2 pl-3 border-l-2 border-indigo-500 bg-indigo-50/80 p-2 rounded-r-lg text-xs text-indigo-950 font-medium">
                                <div class="flex items-center justify-between text-[10px] text-indigo-600 font-bold mb-0.5">
                                    <span><i class="fa-solid fa-reply mr-1"></i> Control Room Reply:</span>
                                    <span class="text-slate-400 font-normal">${formatMessageTime(r.timestamp)}</span>
                                </div>
                                <div class="font-semibold text-slate-800">"${escapeHtml(r.message)}"</div>
                            </div>
                        `;
                    }).join('');
                })()}
                <div class="flex flex-wrap items-center justify-between gap-2">
                    <div class="flex flex-wrap items-center gap-1.5">
                        ${locateBtn}
                        ${contactBtns}
                        <button type="button" onclick="openControlReplyModal('${escapeHtml(m.unitId)}', '${escapeHtml(m.officer)}', '${escapeHtml(m.unitName)}', '${escapeHtml(m.phone)}', '${escapeHtml(m.message)}')" class="px-2.5 py-1 rounded bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-xs flex items-center gap-1 shadow-xs cursor-pointer transition">
                            <i class="fa-solid fa-reply"></i> Reply
                        </button>
                    </div>
                    ${m.lat && m.lng ? `<span class="text-[10px] text-slate-400 font-mono">GPS: ${Number(m.lat).toFixed(5)}, ${Number(m.lng).toFixed(5)}</span>` : '<span class="text-[10px] text-slate-400">No GPS coords</span>'}
                </div>
            </div>
        `;
    }).join('');
};

function formatMessageTime(ts) {
    if (!ts) return '';
    const diff = Math.floor((Date.now() - ts) / 1000);
    if (diff < 60) return 'Just now';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    const d = new Date(ts);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) + ', ' + d.toLocaleDateString([], { month: 'short', day: 'numeric' });
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// =============================================================
// Control Room to Officer Reply Handlers
// =============================================================
let activeReplyTarget = null;

window.openControlReplyModal = function(unitId, officer, unitName, phone, originalMsg) {
    activeReplyTarget = {
        unitId: unitId || '',
        officer: officer || 'Police Officer',
        unitName: unitName || 'Duty Force',
        phone: phone || '',
        originalMsg: originalMsg || ''
    };

    const modal = document.getElementById('control-reply-modal');
    const infoEl = document.getElementById('reply-modal-officer-info');
    const origEl = document.getElementById('reply-modal-original-msg');
    const inputEl = document.getElementById('control-reply-input');

    if (infoEl) infoEl.innerText = `${activeReplyTarget.officer} ${activeReplyTarget.phone ? '(' + activeReplyTarget.phone + ')' : ''} | ${activeReplyTarget.unitName}`;
    if (origEl) origEl.innerText = `"${activeReplyTarget.originalMsg}"`;
    if (inputEl) inputEl.value = '';

    if (modal) modal.classList.remove('hidden');
    if (inputEl) setTimeout(() => inputEl.focus(), 150);
};

window.closeControlReplyModal = function() {
    const modal = document.getElementById('control-reply-modal');
    if (modal) modal.classList.add('hidden');
    activeReplyTarget = null;
};

window.setControlQuickReply = function(text) {
    const inputEl = document.getElementById('control-reply-input');
    if (inputEl) inputEl.value = text;
};

window.submitControlReply = function() {
    if (!activeReplyTarget) return;

    const inputEl = document.getElementById('control-reply-input');
    const replyText = (inputEl ? inputEl.value : '').trim();
    if (!replyText) {
        alert('Please enter or select a reply message to send.');
        return;
    }

    if (!window.liveMqttClient || !window.liveMqttClient.connected) {
        alert('Control Room MQTT is currently offline. Please wait for reconnection.');
        return;
    }

    const btn = document.getElementById('btn-submit-control-reply');
    const origHtml = btn ? btn.innerHTML : '';
    if (btn) {
        btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Sending...';
        btn.disabled = true;
    }

    const replyPacket = {
        type: 'control_reply',
        replyId: 'rep_' + Date.now() + '_' + Math.random().toString(36).substring(2, 6),
        unitId: activeReplyTarget.unitId,
        officer: activeReplyTarget.officer,
        replyMessage: replyText,
        sentBy: 'Police Control Room',
        timestamp: Date.now()
    };

    const generalReplyTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/control_reply';
    const specificReplyTopic = 'srikakulam_police_tracker/' + LIVE_ROOM_CODE + '/reply/' + activeReplyTarget.unitId;

    try {
        window.liveMqttClient.publish(generalReplyTopic, JSON.stringify(replyPacket), { qos: 1 });
        if (activeReplyTarget.unitId) {
            window.liveMqttClient.publish(specificReplyTopic, JSON.stringify(replyPacket), { qos: 1 });
        }

        // Store reply inside the matching officer message thread
        if (Array.isArray(window.liveOfficerMessages)) {
            const found = window.liveOfficerMessages.find(function(m) {
                return (m.unitId && m.unitId === activeReplyTarget.unitId) || (m.officer === activeReplyTarget.officer);
            });
            if (found) {
                if (!found.replies) found.replies = [];
                found.replies.push({
                    message: replyText,
                    timestamp: Date.now()
                });
                try {
                    localStorage.setItem('gp_officer_messages', JSON.stringify(window.liveOfficerMessages));
                } catch (e) {}
            }
        }

        broadcastMessagesHistory();
        alert('Reply sent successfully to ' + activeReplyTarget.officer + '!\n\nMessage: "' + replyText + '"');
        closeControlReplyModal();
        renderOfficerMessagesList();
    } catch (e) {
        alert('Failed to send reply: ' + e.message);
    } finally {
        if (btn) {
            btn.innerHTML = origHtml;
            btn.disabled = false;
        }
    }
};

// Automatically start Real-time MQTT connection and message storage on page load
if (typeof initOfficerMessagesStorage === 'function') {
    initOfficerMessagesStorage();
}
if (typeof initLiveMQTT === 'function') {
    setTimeout(initLiveMQTT, 300);
}