function statusBadge(status) {
  const map = {
    0: { text: 'Pending', cls: 'secondary' },
    1: { text: 'Assigned', cls: 'warning' },
    2: { text: 'Started', cls: 'primary' },
    3: { text: 'Completed', cls: 'success' },
    4: { text: 'Cancelled', cls: 'danger' }
  };
  const legacyMap = { Pending: 0, Assigned: 1, Started: 2, Completed: 3, Cancelled: 4 };
  const code = Number.isFinite(Number(status)) ? Number(status) : legacyMap[status];
  const meta = map[code] || map[0];
  return `<span class="badge text-bg-${meta.cls}">${meta.text}</span>`;
}

function statusText(status) {
  const labels = { 0: 'Pending', 1: 'Assigned', 2: 'Started', 3: 'Completed', 4: 'Cancelled' };
  const legacyMap = { Pending: 0, Assigned: 1, Started: 2, Completed: 3, Cancelled: 4 };
  const code = Number.isFinite(Number(status)) ? Number(status) : legacyMap[status];
  return labels[code] || 'Pending';
}

function formatVisitDate(value) {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString('en-GB', {
    weekday: 'short', day: '2-digit', month: 'short', year: 'numeric', timeZone: 'UTC'
  });
}

function setDashboardPageLoading(isLoading) {
  if (isLoading) $('#hd-page-loader').removeClass('d-none');
  else $('#hd-page-loader').addClass('d-none');
}

function markDashboardReady() { $('#h-dashboard-page').addClass('is-ready'); }

function todayIsoLocal() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function openAssignBookingSmoothly(e) {
  if (e) e.preventDefault();
  const targetHref = $('#btn-go-assign-booking').attr('href') || '/hhome-collection/assign-booking';
  $('.hd-loader-text').text('Opening Asgn Booking...');
  setDashboardPageLoading(true);
  $('#h-dashboard-page').addClass('is-leaving');
  setTimeout(function () { window.location.href = targetHref; }, 170);
}

function resetDashboardPageState() {
  $('#h-dashboard-page').removeClass('is-leaving').addClass('is-ready');
  $('.hd-loader-text').text('Loading All Booking...');
  setDashboardPageLoading(false);
}

function loadDashboard() {
  const params = {
    date_from: $('#f-date-from').val(),
    date_to: $('#f-date-to').val(),
    status: $('#f-status').val(),
    route: $('#f-route').val(),
    search: $('#f-search').val()
  };

  return $.get('/hhome-collection/dashboard-data', params, function (res) {
    const rows = res.rows || [];
    const html = rows.map((r, idx) => `
      <tr class="dash-data-row">
        <td>${idx + 1}</td>
        <td>${r.row_type === 'APPOINTMENT' ? `[A${r.appointment_no || '-'}] ` : ''}${r.patient_names || r.caller_name || '-'}</td>
        <td>${r.primary_mobile || '-'}</td>
        <td>${formatVisitDate(r.preferred_visit_date)}</td>
        <td>${r.preferred_time_slot || '-'}</td>
        <td>${r.patient_count || 0}</td>
        <td>${r.colony_name_snapshot || r.colony_name || '-'}</td>
        <td>${r.route_no_snapshot || r.route_no || '-'}</td>
        <td>${statusBadge(r.booking_status)}</td>
        <td class="dash-actions-cell">
          <button class="btn btn-sm btn-outline-primary dash-action-btn btn-view" data-booking-id="${r.booking_id || r.id}" data-appointment-id="${r.appointment_id || 0}">View</button>
          ${[3, 4].includes(Number(r.booking_status))
            ? '<button class="btn btn-sm btn-outline-secondary dash-action-btn" type="button" disabled>Modify</button>'
            : `<button class="btn btn-sm btn-outline-secondary dash-action-btn btn-modify" data-booking-id="${r.booking_id || r.id}" data-appointment-id="${r.appointment_id || 0}">Modify</button>`}
          ${r.row_type === 'BOOKING' && Number(r.booking_status) !== 4
            ? `<button class="btn btn-sm btn-outline-info dash-action-btn btn-book-appt" data-booking-id="${r.id}">Book Appt</button>`
            : ''}
          <button class="btn btn-sm btn-outline-warning dash-action-btn btn-assign" data-booking-id="${r.booking_id || r.id}" data-appointment-id="${r.appointment_id || 0}">Assign</button>
          <button class="btn btn-sm btn-outline-danger dash-action-btn btn-cancel" data-booking-id="${r.booking_id || r.id}" data-appointment-id="${r.appointment_id || 0}">Cancel</button>
        </td>
      </tr>
    `).join('');

    $('#dashboard-table tbody').html(html || '<tr class="dash-empty-row"><td colspan="10" class="text-center">No records</td></tr>');
    bindRowActions();
  });
}

function bindAssignForSingleBooking(bookingId) {
  let appointmentId = 0;
  if (typeof bookingId === 'object' && bookingId !== null) {
    appointmentId = Number(bookingId.appointment_id || 0);
    bookingId = Number(bookingId.booking_id || 0);
  }
  $.get('/hhome-collection/phlebotomists', function (res) {
    if (!res.phlebotomists.length) return alert('No active users found with designation "Home Collection Phlebo".');
    const $modalHeader = $('#bookingModal .modal-header');
    const $modalFooter = $('#booking-modal-footer');
    const originalHeaderHtml = $modalHeader.html();

    const rows = (typeof window.renderHcAssignAlphaChipGroups === 'function')
      ? window.renderHcAssignAlphaChipGroups(res.phlebotomists || [], { selectedUserId: 0, assignedSet: new Set() })
      : '';

    $modalHeader.html(`
      <h5 class="modal-title">Assign Phlebotomist</h5>
      <div class="asg-modal-search-wrap">
        <input id="dash-assign-search" type="text" class="form-control form-control-sm" placeholder="Type 2 letters to search">
        <div id="dash-assign-suggest" class="asg-phlebo-search-suggest d-none"></div>
      </div>
      <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
    `);

    $('#booking-modal-body').html(`<div id="assign-chip-list" class="assign-alpha-groups">${rows}</div>`);
    $modalFooter.removeClass('d-none').html('<button id="save-assign" type="button" class="btn btn-warning btn-sm" disabled>Save</button>');
    const m = new bootstrap.Modal(document.getElementById('bookingModal'));
    m.show();

    let selectedUserId = null;
    let pickerCtrl = null;
    function setSelectedUser(uid, name) {
      selectedUserId = Number(uid || 0);
      $('#save-assign').prop('disabled', !selectedUserId);
      if (name) $('#dash-assign-search').val(name);
    }

    if (typeof window.initHcAssignUserPicker === 'function') {
      pickerCtrl = window.initHcAssignUserPicker({
        inputSelector: '#dash-assign-search',
        suggestSelector: '#dash-assign-suggest',
        chipContainerSelector: '#assign-chip-list',
        saveBtnSelector: '#save-assign',
        searchUrl: '/hhome-collection/internal-ref-users',
        limit: 20,
        debounceMs: 200,
        onSelect: function (uid, name) { setSelectedUser(uid, name); }
      });
    }

    $(document.getElementById('bookingModal')).off('hidden.bs.modal.assignPickerCleanup').on('hidden.bs.modal.assignPickerCleanup', function () {
      if (pickerCtrl && typeof pickerCtrl.destroy === 'function') pickerCtrl.destroy();
      $modalHeader.html(originalHeaderHtml);
      $modalFooter.addClass('d-none').html('<button type="button" class="btn btn-primary btn-sm" id="booking-modal-print-btn">Print Slip</button>');
    });

    $('#save-assign').off('click').on('click', function () {
      if (!selectedUserId) return;
      $.ajax({
        url: '/hhome-collection/assign-phlebotomist', method: 'POST', contentType: 'application/json',
        data: JSON.stringify({ booking_id: bookingId, appointment_id: appointmentId, user_id: selectedUserId }),
        success: function () { m.hide(); loadDashboard(); },
        error: function (xhr) { alert(xhr.responseJSON?.message || 'Assign failed'); }
      });
    });
  });
}

function openCancelReasonModal(bookingId) {
  let appointmentId = 0;
  if (typeof bookingId === 'object' && bookingId !== null) {
    appointmentId = Number(bookingId.appointment_id || 0);
    bookingId = Number(bookingId.booking_id || 0);
  }
  const modalEl = document.getElementById('cancelReasonModal');
  if (!modalEl) return;
  const m = new bootstrap.Modal(modalEl);
  $('#cancel-reason-select').val('');
  m.show();
  $('#btn-confirm-cancel-reason').off('click').on('click', function () {
    const reason = ($('#cancel-reason-select').val() || '').trim();
    if (!reason) return alert('Please select cancel reason.');
    $.ajax({
      url: '/hhome-collection/cancel-booking', method: 'POST', contentType: 'application/json',
      data: JSON.stringify({ booking_id: bookingId, appointment_id: appointmentId, reason_text: reason }),
      success: function () { m.hide(); loadDashboard(); },
      error: function (xhr) { alert(xhr.responseJSON?.message || 'Cancel failed'); }
    });
  });
}

function openBookAppointmentReasonModal(bookingId) {
  const modalEl = document.getElementById('modifyReasonModal');
  if (!modalEl) return;
  const m = new bootstrap.Modal(modalEl);
  const $title = $('#modifyReasonModal .modal-title');
  const prevTitle = $title.text();
  $title.text('Book Appointment');
  $('#modify-reason-text').val('');
  m.show();
  $('#btn-open-modify-flow').off('click').on('click', function () {
    const reason = ($('#modify-reason-text').val() || '').trim();
    $.ajax({
      url: '/hhome-collection/book-appointment-init',
      method: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({ booking_id: bookingId, reason_text: reason }),
      success: function (res) {
        m.hide();
        $title.text(prevTitle);
        const target = res?.redirect_url || '/hhome-collection?mode=modify';
        window.location.href = target;
      },
      error: function (xhr) {
        alert(xhr.responseJSON?.message || 'Unable to start appointment flow');
      }
    });
  });
  $(modalEl).off('hidden.bs.modal.bookAppt').on('hidden.bs.modal.bookAppt', function () {
    $title.text(prevTitle);
  });
}

function openModifyReasonModal(bookingTarget) {
  let appointmentId = 0;
  let bookingId = bookingTarget;
  if (typeof bookingTarget === 'object' && bookingTarget !== null) {
    appointmentId = Number(bookingTarget.appointment_id || 0);
    bookingId = Number(bookingTarget.booking_id || 0);
  }
  const modalEl = document.getElementById('modifyReasonModal');
  if (!modalEl) return;
  const m = new bootstrap.Modal(modalEl);
  $('#modify-reason-text').val('');
  m.show();
  $('#btn-open-modify-flow').off('click').on('click', function () {
    const reason = ($('#modify-reason-text').val() || '').trim();
    if (!reason) return alert('Modify reason is required.');

    $.ajax({
      url: '/hhome-collection/modify-init',
      method: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({ booking_id: bookingId, appointment_id: appointmentId, reason_text: reason }),
      success: function (res) {
        m.hide();
        const target = res?.redirect_url || '/hhome-collection?mode=modify';
        window.location.href = target;
      },
      error: function (xhr) {
        alert(xhr.responseJSON?.message || 'Unable to start modify flow');
      }
    });
  });
}

function bindRowActions() {
  $('.btn-view').off('click').on('click', function () {
    const bookingId = Number($(this).data('booking-id') || 0);
    $.get(`/hhome-collection/booking/${bookingId}`, function (res) {
      const b = res.booking;
      $('#bookingModal .modal-title').text('Booking Detail');
      const patients = (b.patients || []).map(p => `
        <li>
          <div><strong>${p.full_name}</strong></div>
          <div class="small text-muted">Tests: ${p.tests_display || '-'}</div>
        </li>
      `).join('');
      $('#booking-modal-body').html(`
        <p><strong>${b.booking_code}</strong> | ${b.preferred_visit_date} ${b.preferred_time_slot}</p>
        <p>Caller: ${b.caller_name} (${b.primary_mobile})</p>
        <p>Address: ${b.house_flat_no}, ${b.floor || ''}, ${b.block_tower_no || ''}, ${b.colony_name_snapshot}</p>
        <p>Status: ${statusText(b.booking_status)}</p>
        <ul>${patients}</ul>
      `);
      $('#booking-modal-footer').removeClass('d-none');
      $('#booking-modal-print-btn').off('click').on('click', function () {
        window.open(`/hhome-collection/print/${b.id}`, '_blank');
      });
      new bootstrap.Modal(document.getElementById('bookingModal')).show();
    });
  });

  $('.btn-assign').off('click').on('click', function () {
    bindAssignForSingleBooking({
      booking_id: Number($(this).data('booking-id') || 0),
      appointment_id: Number($(this).data('appointment-id') || 0),
    });
  });

  $('.btn-cancel').off('click').on('click', function () {
    openCancelReasonModal({
      booking_id: Number($(this).data('booking-id') || 0),
      appointment_id: Number($(this).data('appointment-id') || 0),
    });
  });

  $('.btn-modify').off('click').on('click', function () {
    openModifyReasonModal({
      booking_id: Number($(this).data('booking-id') || 0),
      appointment_id: Number($(this).data('appointment-id') || 0),
    });
  });

  $('.btn-book-appt').off('click').on('click', function () {
    openBookAppointmentReasonModal(Number($(this).data('booking-id') || 0));
  });
}

$(function () {
  if (!$('#dashboard-table').length) return;

  window.addEventListener('pageshow', function () {
    resetDashboardPageState();
  });

  const today = todayIsoLocal();
  if (!$('#f-date-from').val()) $('#f-date-from').val(today);
  if (!$('#f-date-to').val()) $('#f-date-to').val(today);

  setDashboardPageLoading(true);
  $('#btn-go-assign-booking').off('click').on('click', openAssignBookingSmoothly);
  $('#btn-filter').off('click').on('click', function () { loadDashboard(); });

  const req = loadDashboard();
  if (req && typeof req.always === 'function') {
    req.always(function () {
      setDashboardPageLoading(false);
      markDashboardReady();
    });
  } else {
    setDashboardPageLoading(false);
    markDashboardReady();
  }
});
