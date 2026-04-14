(function () {
  const tableEl = document.getElementById('asg-grid-table');
  if (!tableEl) return;

  const dateEl = document.getElementById('asg-plan-date');
  const metaEl = document.getElementById('asg-meta');
  const gridWrapEl = document.querySelector('.h-assign-grid-wrap');
  const xscrollEl = document.getElementById('asg-xscroll');
  const xscrollInnerEl = document.getElementById('asg-xscroll-inner');
  const modalEl = document.getElementById('asgModal');
  const modal = new bootstrap.Modal(modalEl);
  const modalSearchInputEl = document.getElementById('asg-phlebo-search');
  const commitBtn = document.getElementById('asg-commit-btn');
  const reloadBtn = document.getElementById('asg-reload');
  const toastEl = document.getElementById('asg-feedback-toast');
  const toastBodyEl = document.getElementById('asg-feedback-toast-body');
  const feedbackToast = toastEl ? new bootstrap.Toast(toastEl) : null;
  const commitBtnDefaultHtml = commitBtn ? commitBtn.innerHTML : 'Assign All Bookings';

  const state = {
    routes: [],
    bookings: [],
    phlebos: [],
    routeOrder: [],
    routeAssignee: {},
    currentRouteForAssign: null,
    selectedPhlebo: null,
    draggedBookingId: null,
    isCommitting: false,
  };
  let assignPickerCtrl = null;

  function showFeedbackToast(kind, text) {
    if (!feedbackToast || !toastEl || !toastBodyEl) return;
    toastEl.classList.remove('text-bg-success', 'text-bg-danger', 'text-bg-warning');
    if (kind === 'success') toastEl.classList.add('text-bg-success');
    else if (kind === 'danger') toastEl.classList.add('text-bg-danger');
    else toastEl.classList.add('text-bg-warning');
    toastBodyEl.textContent = text || '';
    feedbackToast.show();
  }

  function setCommitLoading(isLoading) {
    state.isCommitting = !!isLoading;
    if (!commitBtn) return;
    commitBtn.disabled = !!isLoading;
    if (reloadBtn) reloadBtn.disabled = !!isLoading;
    if (isLoading) {
      commitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>Assigning...';
    } else {
      commitBtn.innerHTML = commitBtnDefaultHtml;
    }
  }

  function formatMinutesTo12h(totalMinutes) {
    const mins = ((totalMinutes % 1440) + 1440) % 1440;
    let h24 = Math.floor(mins / 60);
    const m = mins % 60;
    const ap = h24 >= 12 ? 'PM' : 'AM';
    let h12 = h24 % 12;
    if (h12 === 0) h12 = 12;
    return `${String(h12).padStart(2, '0')}:${String(m).padStart(2, '0')} ${ap}`;
  }

  function generateHalfHourSlots() {
    const slots = [];
    for (let mins = 6 * 60; mins < 24 * 60; mins += 30) {
      slots.push(`${formatMinutesTo12h(mins)} to ${formatMinutesTo12h(mins + 30)}`);
    }
    return slots;
  }

  const TIME_SLOTS = generateHalfHourSlots();

  function esc(v) {
    return String(v ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function defaultTomorrow() {
    const d = new Date();
    d.setDate(d.getDate() + 1);
    return d.toISOString().slice(0, 10);
  }

  function phleboName(id) {
    const p = state.phlebos.find(x => Number(x.id) === Number(id));
    return p ? p.full_name : '';
  }

  function countRoute(route) {
    return state.bookings.filter(b => b.route_name === route).length;
  }

  function renderMeta() {
    const total = state.bookings.length;
    const activeRoutes = state.routeOrder.filter(r => countRoute(r) > 0);
    const assigned = activeRoutes.filter(r => !!state.routeAssignee[r]).length;
    metaEl.innerHTML = `
      <span><strong>Total bookings:</strong> ${total}</span>
      <span><strong>Visible routes:</strong> ${state.routeOrder.length}</span>
      <span><strong>Routes assigned:</strong> ${assigned}</span>
    `;
  }

  function renderTable() {
    const routes = [...state.routeOrder];
    const slots = [...TIME_SLOTS];

    let html = '<thead><tr><th class="asg-time-col">Time Slot</th>';
    routes.forEach(route => {
      const uid = state.routeAssignee[route] || '';
      const aname = uid ? phleboName(uid) : 'No phlebo';
      html += `
        <th class="asg-route-th" data-route-id="${esc(route)}">
          <div class="asg-route-head">
            <div class="asg-route-top">
              <div class="asg-route-title" draggable="true" data-route-id="${esc(route)}">${esc(route)}</div>
              <button class="btn btn-sm btn-assign-route" data-route-id="${esc(route)}">Asgn</button>
            </div>
            <div class="asg-route-bottom">
              <div class="asg-route-assignee ${uid ? '' : 'none'}">${esc(aname)}</div>
              <div class="asg-route-count">${countRoute(route)}</div>
            </div>
          </div>
        </th>
      `;
    });
    html += '</tr></thead><tbody>';

    slots.forEach(slot => {
      html += `<tr><td class="asg-time-col">${esc(slot)}</td>`;
      routes.forEach(route => {
        const items = state.bookings.filter(b => b.route_name === route && b.slot === slot);
        html += `<td class="asg-slot-cell" data-route-id="${esc(route)}" data-slot="${esc(slot)}">`;
        if (!items.length) {
          html += '<div class="asg-empty">-</div>';
        } else {
          items.forEach(b => {
            const colony = esc(b.colony_name_snapshot || '');
            const city = esc(b.city || '');
            const place = [colony, city].filter(Boolean).join(', ');
            const callerMobile = esc(b.caller_mobile || '');
            const callerMobileDisplay = callerMobile ? `(${callerMobile})` : '(-)';
            const patientCount = Number(b.patient_count || 0);
            const extraCount = patientCount > 1 ? (patientCount - 1) : 0;
            html += `
              <div class="asg-booking" draggable="true" data-booking-id="${b.booking_id}">
                ${extraCount > 0 ? `<div class="asg-extra-pill">+${extraCount}</div>` : ''}
                <div class="asg-baddr">${place || '-'}</div>
                <div class="asg-bmob">${callerMobileDisplay}</div>
              </div>
            `;
          });
        }
        html += '</td>';
      });
      html += '</tr>';
    });

    html += '</tbody>';
    tableEl.innerHTML = html;
    bindBookingDnD();
    bindColumnDnD();
    bindAssignRouteBtns();
    syncBottomScroller();
  }

  function syncBottomScroller() {
    if (!gridWrapEl || !xscrollEl || !xscrollInnerEl) return;
    xscrollInnerEl.style.width = `${tableEl.scrollWidth}px`;
    xscrollEl.scrollLeft = gridWrapEl.scrollLeft;
  }

  function bindBottomScrollerSync() {
    if (!gridWrapEl || !xscrollEl) return;
    xscrollEl.addEventListener('scroll', () => {
      gridWrapEl.scrollLeft = xscrollEl.scrollLeft;
    });
  }

  function bindBookingDnD() {
    tableEl.querySelectorAll('.asg-booking').forEach(card => {
      card.addEventListener('dragstart', e => {
        state.draggedBookingId = card.dataset.bookingId || null;
        e.dataTransfer.setData('text/booking', state.draggedBookingId || '');
        e.dataTransfer.effectAllowed = 'move';
      });
      card.addEventListener('dragend', () => {
        state.draggedBookingId = null;
        tableEl.querySelectorAll('.asg-slot-cell').forEach(c => c.classList.remove('drop-on'));
      });
    });

    tableEl.querySelectorAll('.asg-slot-cell').forEach(cell => {
      cell.addEventListener('dragover', e => {
        const bookingId = state.draggedBookingId || e.dataTransfer.getData('text/booking');
        if (!bookingId) return;
        const item = state.bookings.find(b => String(b.booking_id) === String(bookingId));
        if (!item) return;
        if (String(item.slot) !== String(cell.dataset.slot)) return; // same-row only
        e.preventDefault();
        cell.classList.add('drop-on');
      });

      cell.addEventListener('dragleave', () => cell.classList.remove('drop-on'));

      cell.addEventListener('drop', e => {
        const bookingId = state.draggedBookingId || e.dataTransfer.getData('text/booking');
        if (!bookingId) return;
        const item = state.bookings.find(b => String(b.booking_id) === String(bookingId));
        if (!item) return;
        if (String(item.slot) !== String(cell.dataset.slot)) return; // block up/down
        e.preventDefault();
        cell.classList.remove('drop-on');
        item.route_name = cell.dataset.routeId; // UI grouping only
        renderAll();
      });
    });
  }

  function bindColumnDnD() {
    tableEl.querySelectorAll('.asg-route-title').forEach(title => {
      title.addEventListener('dragstart', e => {
        e.dataTransfer.setData('text/route', title.dataset.routeId);
        e.dataTransfer.effectAllowed = 'move';
      });
    });

    tableEl.querySelectorAll('.asg-route-th').forEach(th => {
      th.addEventListener('dragover', e => {
        if (e.dataTransfer.types.includes('text/route')) e.preventDefault();
      });
      th.addEventListener('drop', e => {
        const src = e.dataTransfer.getData('text/route');
        const tgt = th.dataset.routeId;
        if (!src || !tgt || src === tgt) return;
        const from = state.routeOrder.indexOf(src);
        const to = state.routeOrder.indexOf(tgt);
        if (from < 0 || to < 0) return;
        state.routeOrder.splice(from, 1);
        state.routeOrder.splice(to, 0, src);
        renderAll();
      });
    });
  }

  function bindAssignRouteBtns() {
    tableEl.querySelectorAll('.btn-assign-route').forEach(btn => {
      btn.addEventListener('click', () => openAssignModal(btn.dataset.routeId));
    });
  }

  function setSelectedPhlebo(userId, displayName = '') {
    const uid = Number(userId || 0);
    if (!uid) return;
    const cleanName = String(displayName || '').trim();
    const idx = (state.phlebos || []).findIndex(x => Number(x.id) === uid);
    if (idx < 0 && cleanName) {
      state.phlebos.push({ id: uid, full_name: cleanName });
    } else if (idx >= 0 && cleanName && !String(state.phlebos[idx].full_name || '').trim()) {
      state.phlebos[idx].full_name = cleanName;
    }
    state.selectedPhlebo = uid;
    const list = document.getElementById('asg-phlebo-list');
    if (list) {
      list.querySelectorAll('.assign-chip').forEach(x => x.classList.remove('active'));
      const selectedChip = list.querySelector(`.assign-chip[data-user-id="${uid}"]`);
      if (selectedChip) selectedChip.classList.add('active');
    }
    const saveBtn = document.getElementById('asg-save-route-btn');
    if (saveBtn) saveBtn.disabled = false;
  }

  function openAssignModal(routeId) {
    state.currentRouteForAssign = routeId;
    state.selectedPhlebo = state.routeAssignee[routeId] || null;
    document.getElementById('asg-modal-title').textContent = `Assign ${routeId}`;
    const globallyAssigned = new Set(
      Object.values(state.routeAssignee)
        .map(v => Number(v))
        .filter(v => Number.isFinite(v) && v > 0)
    );

    const html = (typeof window.renderHcAssignAlphaChipGroups === 'function')
      ? window.renderHcAssignAlphaChipGroups(state.phlebos || [], {
        selectedUserId: Number(state.selectedPhlebo || 0),
        assignedSet: globallyAssigned,
      })
      : '';
    document.getElementById('asg-phlebo-list').innerHTML = html;
    const saveBtn = document.getElementById('asg-save-route-btn');
    saveBtn.disabled = !state.selectedPhlebo;
    if (modalSearchInputEl) modalSearchInputEl.value = '';
    if (assignPickerCtrl && typeof assignPickerCtrl.hideSuggest === 'function') {
      assignPickerCtrl.hideSuggest();
    }
    modal.show();
  }

  function bindModalEvents() {
    const saveBtn = document.getElementById('asg-save-route-btn');
    if (typeof window.initHcAssignUserPicker === 'function') {
      assignPickerCtrl = window.initHcAssignUserPicker({
        inputSelector: '#asg-phlebo-search',
        suggestSelector: '#asg-phlebo-search-suggest',
        chipContainerSelector: '#asg-phlebo-list',
        saveBtnSelector: '#asg-save-route-btn',
        searchUrl: '/hhome-collection/internal-ref-users',
        limit: 20,
        debounceMs: 200,
        onSelect: function (uid, name) {
          setSelectedPhlebo(uid, name);
        },
      });
    }

    saveBtn.addEventListener('click', () => {
      if (!state.currentRouteForAssign || !state.selectedPhlebo) return;
      state.routeAssignee[state.currentRouteForAssign] = state.selectedPhlebo;
      modal.hide();
      renderAll();
    });
  }

  function buildRouteAssigneeFromRows() {
    const grouped = {};
    state.bookings.forEach(b => {
      const r = b.route_name;
      grouped[r] = grouped[r] || [];
      if (b.assigned_user_id) grouped[r].push(Number(b.assigned_user_id));
    });
    const routeAssignee = {};
    Object.keys(grouped).forEach(route => {
      const uniq = Array.from(new Set(grouped[route]));
      routeAssignee[route] = uniq.length === 1 ? uniq[0] : null;
    });
    state.routeAssignee = routeAssignee;
  }

  function loadPlanner() {
    const d = dateEl.value || defaultTomorrow();
    $.get('/hhome-collection/assign-booking-data', { date: d }, function (res) {
      if (!res?.ok) {
        showFeedbackToast('danger', res?.message || 'Unable to load assignment planner');
        return;
      }
      state.routes = res.routes || [];
      state.bookings = res.rows || [];
      state.phlebos = res.phlebos || [];
      state.routeOrder = [...state.routes];
      buildRouteAssigneeFromRows();
      renderAll();
    }).fail(function (xhr) {
      showFeedbackToast('danger', xhr.responseJSON?.message || 'Unable to load assignment planner');
    });
  }

  function commitAssignments() {
    if (state.isCommitting) return;

    const activeRoutes = state.routeOrder.filter(r => countRoute(r) > 0);
    const pendingRoutes = activeRoutes.filter(r => !state.routeAssignee[r]);
    if (pendingRoutes.length) {
      showFeedbackToast('warning', `Please assign phlebotomist for all routes. Pending: ${pendingRoutes.join(', ')}`);
      return;
    }

    const assignments = state.bookings.map(b => ({
      booking_id: b.booking_id,
      grouped_route: b.route_name,
      assigned_user_id: state.routeAssignee[b.route_name]
    }));

    setCommitLoading(true);
    $.ajax({
      url: '/hhome-collection/assign-bookings-commit',
      method: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({
        plan_date: dateEl.value || defaultTomorrow(),
        assignments
      }),
      success: function (res) {
        showFeedbackToast('success', `Assigned successfully. Updated bookings: ${res.updated_count || 0}`);
        // message preview returned by backend if needed in future
        console.log('assignment messages_preview', res.messages_preview || []);
        loadPlanner();
      },
      error: function (xhr) {
        showFeedbackToast('danger', xhr.responseJSON?.message || 'Assignment failed');
      },
      complete: function () {
        setCommitLoading(false);
      }
    });
  }

  function renderAll() {
    renderMeta();
    renderTable();
  }

  function init() {
    dateEl.value = dateEl.value || window.H_ASSIGN_DEFAULT_DATE || defaultTomorrow();
    bindModalEvents();
    bindBottomScrollerSync();
    if (reloadBtn) reloadBtn.addEventListener('click', loadPlanner);
    if (commitBtn) commitBtn.addEventListener('click', commitAssignments);
    loadPlanner();
  }

  init();
})();
