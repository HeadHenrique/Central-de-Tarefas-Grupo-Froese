(function () {
  'use strict';

  const SUPABASE_URL = 'https://zkgclcthbubqhqwbjrhy.supabase.co';
  const SUPABASE_KEY = 'sb_publishable_xmFLVtt8PGhCNAtn9uDiGQ_5RRbnIkC';

  if (!window.supabase || !window.supabase.createClient) {
    throw new Error('Supabase JS não carregado.');
  }

  const client = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: {
      persistSession: true,
      autoRefreshToken: true,
      detectSessionInUrl: true
    }
  });

  const clone = v => JSON.parse(JSON.stringify(v == null ? {} : v));
  const emailNorm = v => String(v || '').trim().toLowerCase();
  const makeId = () => {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
  };

  function mapError(error) {
    const e = new Error((error && error.message) || 'Erro no banco.');
    e.code = (error && (error.code || error.status)) || 'db_error';
    return e;
  }

  function docSnapshot(row) {
    return {
      id: row.id,
      exists: true,
      data: () => clone(row.data || {})
    };
  }

  function querySnapshot(rows) {
    return { docs: (rows || []).map(docSnapshot) };
  }

  async function fetchRows(collection, where) {
    let q = client.from('documents').select('id,data').eq('collection', collection);
    if (where) {
      const [field, op, value] = where;
      if (op !== '==') throw new Error('Operador não suportado: ' + op);
      q = q.eq('data->>' + field, String(value));
    }
    const { data, error } = await q;
    if (error) throw mapError(error);
    return data || [];
  }

  function subscribe(collection, where, next, errorCb) {
    let alive = true;
    let timer = null;
    const emit = async () => {
      clearTimeout(timer);
      timer = setTimeout(async () => {
        if (!alive) return;
        try {
          const rows = await fetchRows(collection, where);
          if (alive) next(querySnapshot(rows));
        } catch (e) {
          if (alive && errorCb) errorCb(e);
        }
      }, 30);
    };

    emit();
    const channel = client
      .channel('docs-' + collection + '-' + makeId())
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table: 'documents', filter: 'collection=eq.' + collection },
        emit
      )
      .subscribe();

    return () => {
      alive = false;
      clearTimeout(timer);
      client.removeChannel(channel);
    };
  }

  function documentRef(collection, id) {
    return {
      id,
      async get() {
        const { data, error } = await client
          .from('documents')
          .select('id,data')
          .eq('collection', collection)
          .eq('id', id)
          .maybeSingle();
        if (error) throw mapError(error);
        if (!data) return { id, exists: false, data: () => undefined };
        return docSnapshot(data);
      },
      async set(value) {
        const { error } = await client.from('documents').upsert({
          collection,
          id,
          data: clone(value),
          updated_at: new Date().toISOString()
        }, { onConflict: 'collection,id' });
        if (error) throw mapError(error);
      },
      async update(patch) {
        const { error } = await client.rpc('merge_document', {
          p_collection: collection,
          p_id: id,
          p_patch: clone(patch)
        });
        if (error) throw mapError(error);
      },
      async delete() {
        const { error } = await client
          .from('documents')
          .delete()
          .eq('collection', collection)
          .eq('id', id);
        if (error) throw mapError(error);
      },
      onSnapshot(next, errorCb) {
        let alive = true;
        let timer = null;
        const emit = async () => {
          clearTimeout(timer);
          timer = setTimeout(async () => {
            if (!alive) return;
            try {
              const snap = await this.get();
              if (alive) next(snap);
            } catch (e) {
              if (alive && errorCb) errorCb(e);
            }
          }, 30);
        };
        emit();
        const channel = client
          .channel('doc-' + collection + '-' + id + '-' + makeId())
          .on(
            'postgres_changes',
            { event: '*', schema: 'public', table: 'documents', filter: 'collection=eq.' + collection },
            payload => {
              const row = payload.new && payload.new.id ? payload.new : payload.old;
              if (!row || row.id === id) emit();
            }
          )
          .subscribe();
        return () => {
          alive = false;
          clearTimeout(timer);
          client.removeChannel(channel);
        };
      }
    };
  }

  function collectionRef(name) {
    return {
      doc(id) {
        return documentRef(name, id || makeId());
      },
      async add(value) {
        const ref = documentRef(name, makeId());
        await ref.set(value);
        return ref;
      },
      where(field, op, value) {
        const where = [field, op, value];
        return {
          async get() {
            return querySnapshot(await fetchRows(name, where));
          },
          onSnapshot(next, err) {
            return subscribe(name, where, next, err);
          }
        };
      },
      onSnapshot(next, err) {
        return subscribe(name, null, next, err);
      }
    };
  }

  const db = {
    collection: collectionRef,
    doc(path) {
      const i = String(path).indexOf('/');
      if (i < 1) throw new Error('Caminho inválido: ' + path);
      return documentRef(path.slice(0, i), path.slice(i + 1));
    }
  };

  async function accessFor(email) {
    const { data, error } = await client
      .from('app_users')
      .select('email,role,active,auth_user_id')
      .ilike('email', emailNorm(email))
      .maybeSingle();
    if (error) throw mapError(error);
    return data || null;
  }

  async function syncAccess(email, role, active) {
    email = emailNorm(email);
    if (!email) return;
    const { error } = await client.from('app_users').upsert({
      email,
      role: role === 'admin' ? 'admin' : role === 'gestor' ? 'gestor' : 'colaborador',
      active: active !== false,
      updated_at: new Date().toISOString()
    }, { onConflict: 'email' });
    if (error) throw mapError(error);
  }

  async function removeAccess(email) {
    email = emailNorm(email);
    if (!email) return;
    const { error } = await client.from('app_users').delete().eq('email', email);
    if (error) throw mapError(error);
  }

  async function linkPerson(personId) {
    const { data, error } = await client.rpc('link_person_self', { p_person_id: personId });
    if (error) throw mapError(error);
    return !!data;
  }

  const userAdapter = {
    async id() {
      const { data } = await client.auth.getUser();
      return data && data.user ? data.user.id : null;
    },
    async isOwner() {
      const { data } = await client.auth.getUser();
      const email = data && data.user && data.user.email;
      if (!email) return false;
      const a = await accessFor(email);
      return !!a && a.active !== false && a.role === 'admin';
    },
    async profiles(ids) {
      const out = {};
      if (!Array.isArray(ids) || !ids.length) return out;
      const { data, error } = await client
        .from('app_users')
        .select('auth_user_id,email')
        .in('auth_user_id', ids);
      if (!error) (data || []).forEach(x => {
        if (x.auth_user_id) out[x.auth_user_id] = { name: x.email };
      });
      return out;
    }
  };

  function injectAuthUI() {
    if (document.getElementById('authGate')) return;
    const style = document.createElement('style');
    style.textContent = `
      .auth-gate{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;background:#f3f4f7;font-family:"Archivo",system-ui,-apple-system,"Segoe UI",sans-serif;color:#151a24}
      .auth-card{width:min(420px,100%);background:#fff;border:1px solid #d9dde5;border-radius:16px;padding:28px;box-shadow:0 20px 50px rgba(20,28,45,.12)}
      .auth-brand{font-size:13px;color:#6b7280;margin-bottom:4px}.auth-card h1{font-size:24px;margin:0 0 6px}.auth-card p{color:#667085;margin:0 0 22px;line-height:1.5}
      .auth-field{display:flex;flex-direction:column;gap:6px;margin-bottom:12px}.auth-field label{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;color:#667085}
      .auth-field input{border:1px solid #d0d5dd;border-radius:9px;padding:11px 12px;font:inherit;outline:none}.auth-field input:focus{border-color:#5f5587;box-shadow:0 0 0 3px rgba(95,85,135,.12)}
      .auth-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px}.auth-btn{border:1px solid #d0d5dd;background:#fff;border-radius:9px;padding:10px 14px;font-weight:700;cursor:pointer}
      .auth-btn.primary{background:#5f5587;border-color:#5f5587;color:#fff}.auth-btn:disabled{opacity:.55;cursor:not-allowed}.auth-msg{min-height:20px;margin-top:12px;font-size:13px;color:#667085}
      .auth-msg.err{color:#b42318}.auth-msg.ok{color:#067647}.auth-denied{text-align:center}.auth-denied strong{display:block;font-size:18px;margin-bottom:8px}
      @media (prefers-color-scheme:dark){.auth-gate{background:#0e1218;color:#e6e9ef}.auth-card{background:#161b23;border-color:#2a313d}.auth-card p,.auth-brand,.auth-msg,.auth-field label{color:#98a1b3}.auth-field input,.auth-btn{background:#1d232d;border-color:#344054;color:#e6e9ef}}
    `;
    document.head.appendChild(style);

    const gate = document.createElement('div');
    gate.id = 'authGate';
    gate.className = 'auth-gate';
    gate.hidden = true;
    gate.innerHTML = `
      <div class="auth-card">
        <div class="auth-brand">Grupo Froese</div>
        <h1>Central de Tarefas</h1>
        <p>Entre com seu e-mail e senha. No primeiro acesso, crie sua conta usando o mesmo e-mail cadastrado pela administração.</p>
        <form id="authForm">
          <div class="auth-field"><label for="authEmail">E-mail</label><input id="authEmail" type="email" autocomplete="email" required></div>
          <div class="auth-field"><label for="authPass">Senha</label><input id="authPass" type="password" autocomplete="current-password" minlength="6" required></div>
          <div class="auth-actions">
            <button class="auth-btn primary" id="authLogin" type="submit">Entrar</button>
            <button class="auth-btn" id="authSignup" type="button">Criar primeiro acesso</button>
          </div>
          <div class="auth-msg" id="authMsg"></div>
        </form>
      </div>`;
    document.body.insertBefore(gate, document.body.firstChild);

    const form = gate.querySelector('#authForm');
    const email = gate.querySelector('#authEmail');
    const pass = gate.querySelector('#authPass');
    const msg = gate.querySelector('#authMsg');
    const login = gate.querySelector('#authLogin');
    const signup = gate.querySelector('#authSignup');

    const setBusy = v => { login.disabled = v; signup.disabled = v; };
    const show = (text, type) => { msg.textContent = text || ''; msg.className = 'auth-msg' + (type ? ' ' + type : ''); };

    form.addEventListener('submit', async e => {
      e.preventDefault();
      show('');
      setBusy(true);
      const { error } = await client.auth.signInWithPassword({
        email: emailNorm(email.value),
        password: pass.value
      });
      setBusy(false);
      if (error) return show('E-mail ou senha inválidos, ou o e-mail ainda não foi confirmado.', 'err');
      location.reload();
    });

    signup.addEventListener('click', async () => {
      if (!email.value || pass.value.length < 6) {
        show('Informe um e-mail válido e uma senha com pelo menos 6 caracteres.', 'err');
        return;
      }
      show('');
      setBusy(true);
      const { data, error } = await client.auth.signUp({
        email: emailNorm(email.value),
        password: pass.value
      });
      setBusy(false);
      if (error) return show(error.message || 'Não foi possível criar o acesso.', 'err');
      if (data && data.session) {
        location.reload();
        return;
      }
      show('Acesso criado. Confira o e-mail de confirmação e depois volte para entrar.', 'ok');
    });
  }

  function setAppVisible(visible) {
    const wrap = document.querySelector('.wrap');
    if (wrap) wrap.hidden = !visible;
    const gate = document.getElementById('authGate');
    if (gate) gate.hidden = visible;
  }

  function showDenied(email) {
    injectAuthUI();
    setAppVisible(false);
    const gate = document.getElementById('authGate');
    gate.innerHTML = `
      <div class="auth-card auth-denied">
        <div class="auth-brand">Grupo Froese</div>
        <strong>Acesso não liberado</strong>
        <p>O e-mail <b>${String(email || '')}</b> entrou no Supabase, mas ainda não está autorizado na Central de Tarefas.</p>
        <button class="auth-btn" id="authExit" type="button">Sair</button>
      </div>`;
    gate.querySelector('#authExit').onclick = async () => {
      await client.auth.signOut();
      location.reload();
    };
  }

  async function init() {
    injectAuthUI();
    const { data: { session }, error } = await client.auth.getSession();
    if (error || !session || !session.user) {
      setAppVisible(false);
      return { authenticated: false };
    }

    const email = emailNorm(session.user.email);
    const access = await accessFor(email);
    if (!access || access.active === false) {
      showDenied(email);
      return { authenticated: false, denied: true };
    }

    setAppVisible(true);
    return {
      authenticated: true,
      db,
      user: userAdapter,
      userId: session.user.id,
      email,
      role: access.role,
      client
    };
  }

  async function signOut() {
    await client.auth.signOut();
    location.reload();
  }

  window.froeseBackend = {
    client,
    db,
    init,
    signOut,
    syncAccess,
    removeAccess,
    linkPerson,
    accessFor
  };
})();